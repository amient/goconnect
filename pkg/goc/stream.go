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
	Type          reflect.Type
	fn            Fn
	up            *Stream
	pipeline      *Pipeline
	stage         int
	output        OutputChannel
	runner        func(output OutputChannel)
	isPassthrough bool
	pending       chan *Element
	acks          chan Stamp
	terminate     chan bool
	terminating   bool
	completed     chan bool
	closed        bool
	cap           int
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
		if reflect.TypeOf(f).Kind() != reflect.Interface {
			panic(fmt.Errorf("only on of the interfaces defined in god/fn.go  can be applied"))
		}

		panic(fmt.Errorf("reflective transforms need: a) stream.Transform to have guaranteees and b) perf-tested", reflect.TypeOf(f)))
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

	if !stream.Type.AssignableTo(fnType.In(0)) {
		return stream.pipeline.injectCoder(stream, fnType.In(0)).Map(f)
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

func (stream *Stream) log(f string, args ... interface{}) {
	//if stream.stage == 3 {
	log.Printf(f, args...)
	//}
}

func (stream *Stream) pendingAck(element *Element) {
	//stream.log("Incoming %d", element.Stamp)
	element.ack = stream.ack
	if !stream.isPassthrough {
		stream.pending <- element
	}
}

func (stream *Stream) ack(s Stamp) {
	if stream.isPassthrough {
		stream.up.ack(s)
	} else {
		stream.acks <- s
	}
}

func (stream *Stream) close() {
	if ! stream.closed {
		stream.log("Closing Stage %d %v", stream.stage, stream.Type)
		close(stream.output)
		if fn, ok := stream.fn.(Closeable); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}
		stream.closed = true
	}
}

func (stream *Stream) initialize(stage int) {
	if stream.output != nil {
		panic(fmt.Errorf("stream already materialized"))
	}
	log.Printf("Initializing Stage %d %v \n", stage, stream.Type)
	stream.stage = stage
	//TODO configurable capacity for checkpoint buffers
	stream.cap = 1000
	stream.output = make(chan *Element, 1)
	stream.pending = make(chan *Element, 1)
	stream.acks = make(chan Stamp, stream.cap)
	stream.terminate = make(chan bool, 1)
	stream.completed = make(chan bool, 1)
	commits := make(chan map[int]interface{})
	commitRequests := make(chan bool)
	var commitable Commitable
	var isCommitable bool
	if stream.fn == nil {
		stream.isPassthrough = true
	} else {
		_, stream.isPassthrough = stream.fn.(MapFn)
		commitable, isCommitable = stream.fn.(Commitable)
	}

	//start start the ack-commit accumulator that applies:
	//A. accumulation and aggregation of checkpoints when commits are expensive
	//B. back-pressure on the upstream processing when the commits are too slow given the configured buffer size

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
		suspendable := stream.pending
		terminated := false
		pendingAcks := make(map[int][]Stamp, 10)
		acked := make(map[Stamp]bool, stream.cap)
		pendingChks := make(map[Stamp]interface{}, stream.cap)
		checkpoint := make(map[int]interface{})
		pendingCommitReuqest := false
		//ackedKeys := func() []Stamp {
		//	keys := make([]Stamp, 0, len(acked))
		//	for k := range acked {
		//		keys  = append(keys , k)
		//	}
		//	return keys
		//}

		doCommit := func() {
			if len(checkpoint) > 0 {
				pendingCommitReuqest = false
				if isCommitable {
					//stream.log("Commit %d", stream.stage)
					commits <- checkpoint
				}
				checkpoint = make(map[int]interface{})
			}
		}

		accumulate := func() {
			for part, pending := range pendingAcks {
				var upto Stamp
				for ; len(pending) > 0 && acked[pending[0]]; {
					s := pending[0]
					if s > upto {
						upto = s
						checkpoint[part] = pendingChks[upto]
					}
					//stream.log("STAGE[%d] PART: %d DLETING STAMP %d \n", stream.stage, part, s)
					delete(acked, s)
					delete(pendingChks, s)
					pending = pending[1:]
				}
				pendingAcks[part] = pending
			}
			if len(pendingChks) < stream.cap && suspendable == nil {
				//release the backpressure after capacity is freed
				suspendable = stream.pending
			} else if !isCommitable {
				doCommit()
			}
		}

		maybeTerminate := func() {
			if stream.terminating {
				if len(checkpoint) == 0 {
					clean := pendingCommitReuqest || !isCommitable
					for _, p := range pendingAcks {
						clean = clean && len(p) == 0
					}
					if clean {
						terminated = true
					}
				}
			}
			if terminated {
				stream.log("Completed Stage %d", stream.stage)
				close(commits)
				close(commitRequests)
				close(stream.acks)
				close(stream.pending)
			} else {
				//stream.log("Terminating %d (pending commit %d) (pending acks %v)", stream.stage, checkpoint, pendingAcks)
			}
		}

		for !terminated {
			select {
			case <-stream.terminate:
				stream.terminating = true
				maybeTerminate()
			case <-commitRequests:
				pendingCommitReuqest = true
				doCommit()
				maybeTerminate()
			case stamp := <-stream.acks:
				//this doesn't have to block because it doesn't create any memory build-up, if anything it frees memory
				acked[stamp] = true
				accumulate()
				if pendingCommitReuqest {
					doCommit()
				}
				if stream.up != nil {
					stream.up.ack(stamp)
				}
				maybeTerminate()
				//stream.log("STAGE[%d] ACK(%d) InProgress: %v Acked: %v\n", stream.stage, stamp, pendingAcks, ackedKeys())

			case e := <-suspendable:
				part := e.Checkpoint.Part
				var ok bool
				if _, ok = pendingAcks[part]; !ok {
					pendingAcks[part] = make([]Stamp, 0, stream.cap)
				}
				pendingAcks[part] = append(pendingAcks[part], e.Stamp)
				pendingChks[e.Stamp] = e.Checkpoint.Data
				accumulate()
				maybeTerminate()
				//stream.log("STAGE[%d] PENDING(%d) pendingAcks: %v acked: %v\n", stream.stage, e.Stamp, pendingAcks, ackedKeys())
				if len(pendingChks) == stream.cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second suspendable
					suspendable = nil
					//stream.log("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingChks))
				} else if len(pendingChks) > stream.cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", stream.cap))
				}

			}
		}
		stream.completed <- true
	}()

}
