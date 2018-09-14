/*
 * Copyright 2018 Amient Ltd, London
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package goc

import (
	"fmt"
	"log"
	"reflect"
)

type Stream struct {
	Type        reflect.Type
	fn          Fn
	up          *Stream
	pipeline    *Pipeline
	stage       int
	output      OutputChannel
	runner      func(output OutputChannel)
	pending     chan *Element
	acks        chan Stamp
	terminating bool
	completed   chan bool
	closed      bool
	cap         int
}

func (stream *Stream) initialize(stage int) {
	if stream.output != nil {
		panic(fmt.Errorf("stream already materialized"))
	}
	stream.stage = stage
	//TODO configurable capacity for checkpoint buffers
	stream.cap = 1000
	stream.output = make(chan *Element, stream.cap)
	stream.pending = make(chan *Element, stream.cap)
	stream.acks = make(chan Stamp, stream.cap)
	stream.completed = make(chan bool, 1)
	commits := make(chan map[int]interface{})
	commitRequests := make(chan bool)
	commitable, isCommitable := stream.fn.(Commitable)

	//start start committer and commit accumulator that interact with backpressure

	if isCommitable {
		go func() {
			commitRequests <- true
			for checkpoint := range commits {
				commitable.Commit(checkpoint)
				commitRequests <- true
			}
		}()
	}

	go func() {
		var swap chan *Element = stream.pending
		pendingAcks := make(map[int][]Stamp, 10)
		acked := make(map[Stamp]bool, stream.cap)
		pendingChks := make(map[Stamp]interface{}, stream.cap)
		checkpoint := make(map[int]interface{})
		pendingCommitReuqest := false
		maybeTerminate := func() bool {
			if stream.terminating {
				clean := len(checkpoint) == 0
				for _, p := range pendingAcks {
					clean = clean && len(p) == 0
				}
				if clean {
					close(commits)
					stream.completed <- true
					return true
				}
			}
			return false
		}
		doCommit := func() {
			if len(checkpoint) > 0 {
				pendingCommitReuqest = false
				if isCommitable {
					commits <- checkpoint
				}
				checkpoint = make(map[int]interface{})
				if swap == nil {
					log.Printf("STAGE[%d] Releasing backpressure\n", stream.stage)
					swap = stream.pending
				}
			}
		}
		for {
			select {
			case <-commitRequests:
				pendingCommitReuqest = true
				doCommit()
				if maybeTerminate() {
					return
				}
			case stamp := <-stream.acks:
				//this doesn't have to block because it doesn't create any memory build-up, if anything it frees memory
				acked[stamp] = true
				for part, pending := range pendingAcks {
					var upto Stamp
					//log.Printf("STAGE[%d] PART: %d len: %d, acked %d \n", stream.stage, part, len(pendingAcks), acked )
					for ; len(pending) > 0 && acked[pending[0]]; {
						s := pending[0]
						if s > upto {
							upto = s
							checkpoint[part] = pendingChks[upto]
						}
						delete(acked, s)
						delete(pendingChks, s)
						pending = pending[1:]
					}
					pendingAcks[part] = pending
				}

				ackedKeys := make([]Stamp, 0, len(acked))
				for k := range acked {
					ackedKeys = append(ackedKeys, k)
				}
				//log.Printf("STAGE[%d] ACK(%d) InProgress: %v Acked: %v\n", stream.stage, stamp, pendingAcks, ackedKeys)

				if pendingCommitReuqest {
					doCommit()
				}
				if stream.up != nil {
					stream.up.ack(stamp)
				}

				if maybeTerminate() {
					return
				}
			case e := <-swap:
				part := e.Checkpoint.Part
				var ok bool
				if _, ok = pendingAcks[part]; !ok {
					pendingAcks[part] = make([]Stamp, 0, stream.cap)
				}
				pendingAcks[part] = append(pendingAcks[part], e.Stamp)
				pendingChks[e.Stamp] = e.Checkpoint.Data
				if len(pendingChks) == stream.cap {
					log.Printf("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingChks))
					//TODO in order to apply backpressure this channel needs to be nilld but right now it hangs after second swap
					//swap = nil
				} else if len(pendingChks) > stream.cap {
					//panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", stream.cap))
				}
				//log.Printf("STAGE[%d] PENDING(%d) pendingAck: %v\n", stream.stage, e.Stamp, stream.pending)
			}
		}
	}()

}

func (stream *Stream) pendingAck(element *Element) {
	element.ack = stream.ack
	stream.pending <- element
}

func (stream *Stream) ack(s Stamp) {
	stream.acks <- s
}

func (stream *Stream) close() {
	if ! stream.closed {
		log.Println("Closing Stage", stream.stage, stream.Type)
		close(stream.output)
		if fn, ok := stream.fn.(Closeable); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}
		stream.closed = true
	}
}

func (stream *Stream) Apply(f Fn) *Stream {
	switch fn := f.(type) {
	case ForEachFn:
		return stream.pipeline.ForEach(stream, fn)
	case MapFn:
		return stream.pipeline.Map(stream, fn)
	case FlatMapFn:
		return stream.pipeline.FlatMap(stream, fn)
		//case TransformFn:
		//	return stream.pipeline.Transform(stream, fn)
	default:
		panic(fmt.Errorf("reflective transforms need: a) stream.Transform to have guaranteees and b) perf-tested"))
		//if method, exists := reflect.TypeOf(f).MethodByName("process"); !exists {
		//	panic(fmt.Errorf("transform must provide process method"))
		//} else {
		//	v := reflect.ValueOf(f)
		//	args := make([]reflect.Type, method.Type.NumIn()-1)
		//	for i := 1; i < method.Type.NumIn(); i++ {
		//		args[i-1] = method.Type.In(i)
		//	}
		//	ret := make([]reflect.Type, method.Type.NumOut())
		//	for i := 0; i < method.Type.NumOut(); i++ {
		//		ret[i] = method.Type.Out(i)
		//	}
		//	fn := reflect.MakeFunc(reflect.FuncOf(args, ret, false), func(args []reflect.Data) (results []reflect.Data) {
		//		methodArgs := append([]reflect.Data{v}, args...)
		//		return method.Func.Call(methodArgs)
		//	})
		//
		//	var output *Stream
		//	if len(ret) > 1 {
		//		panic(fmt.Errorf("transform must have 0 or 1 return value"))
		//	} else if len(ret) == 0 {
		//		output = stream.Transform(fn)
		//	} else {
		//		output = stream.Map(fn.Interface())
		//	}
		//	output.fn = f
		//	return output
		//}

	}
}

func (stream *Stream) Map(f interface{}) *Stream {

	fnType := reflect.TypeOf(f)
	fnVal := reflect.ValueOf(f)

	if fnType.NumIn() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 input argument"))
	}

	//TODO this check will deferred on after network and type coders injection
	if !stream.Type.AssignableTo(fnType.In(0)) {
		panic(fmt.Errorf("cannot use Map func with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}

	if fnType.NumOut() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 output"))
	}

	return stream.pipeline.elementWise(stream, fnType.Out(0), nil, func(input *Element, output OutputChannel) {
		v := reflect.ValueOf(input.Value)
		r := fnVal.Call([]reflect.Value{v})
		output <- &Element{
			Value:     r[0].Interface(),
			Timestamp: input.Timestamp,
			Stamp:     input.Stamp,
		}
	})

}

func (stream *Stream) Filter(f interface{}) *Stream {

	fnType := reflect.TypeOf(f)
	fnVal := reflect.ValueOf(f)

	if fnType.NumIn() != 1 {
		panic(fmt.Errorf("filter func must have exactly 1 input argument of type %q", stream.Type))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(fnType.In(0)) {
		panic(fmt.Errorf("cannot us FnEW with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}
	if fnType.NumOut() != 1 || fnType.Out(0).Kind() != reflect.Bool {
		panic(fmt.Errorf("FnVal must have exactly 1 output and it should be bool"))
	}

	return stream.pipeline.elementWise(stream, fnType.In(0), nil, func(input *Element, output OutputChannel) {
		v := reflect.ValueOf(input.Value)
		if fnVal.Call([]reflect.Value{v})[0].Bool() {
			output <- input
		} else {
			input.Ack()
		}
	})

}

//func (stream *Stream) Transform(f interface{}) *Stream {
//
//	fnType := reflect.TypeOf(f)
//	fnVal := reflect.ValueOf(f)
//
//	if fnType.NumIn() != 2 || fnType.NumOut() != 0 {
//		panic(fmt.Errorf("sideEffect func must have zero return values and exatly 2 arguments: input underlying and an output underlying"))
//	}
//
//	inChannelType := fnType.In(0)
//	outChannelType := fnType.In(1)
//	if inChannelType.Kind() != reflect.Chan {
//		panic(fmt.Errorf("sideEffect func type input argument must be a chnnel"))
//	}
//
//	if outChannelType.Kind() != reflect.Chan {
//		panic(fmt.Errorf("sideEffect func type output argument must be a chnnel"))
//	}
//
//	//TODO this check will deffered on after network and type coders injection
//	if !stream.Type.AssignableTo(inChannelType.Elem()) {
//		panic(fmt.Errorf("sideEffect func input argument must be a underlying of %q, got underlying of %q", stream.Type, fnType.In(0).Elem()))
//	}
//
//	return stream.pipeline.group(stream, outChannelType.Elem(), nil, func(input InputChannel, output OutputChannel) {
//		intermediateIn := reflect.MakeChan(inChannelType, 0)
//		go func() {
//			defer intermediateIn.Close()
//			for d := range input {
//				intermediateIn.Send(reflect.ValueOf(d.Data))
//			}
//		}()
//
//		intermediateOut := reflect.MakeChan(outChannelType, 0)
//		go func() {
//			defer intermediateOut.Close()
//			fnVal.Call([]reflect.Data{intermediateIn, intermediateOut})
//		}()
//
//		for {
//			o, ok := intermediateOut.Recv()
//			if !ok {
//				return
//			} else {
//				output <- &Element{Data: o.Interface()}
//			}
//		}
//	})
//
//}
