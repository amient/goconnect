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
	case TransformFn:
		return stream.pipeline.Transform(stream, fn)
	case ForEachFn:
		return stream.pipeline.ForEach(stream, fn)
	case MapFn:
		return stream.pipeline.Map(stream, fn)
	case FlatMapFn:
		return stream.pipeline.FlatMap(stream, fn)
	default:
		if reflect.TypeOf(f).Kind() != reflect.Interface {
			panic(fmt.Errorf("only on of the interfaces defined in goc/fn.go  can be applied"))
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

	if !stream.Type.AssignableTo(fnType.In(0)) {
		return stream.pipeline.injectCoder(stream, fnType.In(0)).Filter(f)
	}
	if fnType.NumOut() != 1 || fnType.Out(0).Kind() != reflect.Bool {
		panic(fmt.Errorf("FnVal must have exactly 1 output and it should be bool"))
	}

	var stamp Stamp
	return stream.pipeline.elementWise(stream, fnType.In(0), nil, func(input *Element, output OutputChannel) {
		v := reflect.ValueOf(input.Value)
		if fnVal.Call([]reflect.Value{v})[0].Bool() {
			output <- &Element {
				Stamp: stamp.merge(input.Stamp),
				Value: input.Value,
				Timestamp: input.Timestamp,
				Checkpoint: input.Checkpoint,
			}
			stamp = Stamp{}
		} else {
			stamp = stamp.merge(input.Stamp)
		}
	})

}

//func (stream *Stream) Transform(f func(element *Element, output OutputChannel)) *Stream {
//
//	inField,_ := reflect.TypeOf(f).In(0).Elem().FieldByName("Value")
//	inType := inField.Type
//	//if inType != reflect.TypeOf(&Element{}) {
//	//	panic(fmt.Errorf("custom transform function must have first argument of type *goc.Element"))
//	//}
//	//outChannelType := fnType.In(1)
//	//if outChannelType.Kind() != reflect.Chan {
//	//	panic(fmt.Errorf("sideEffect func type output argument must be a chnnel"))
//	//}
//	//
//	//TODO this check will deffered on after network and type coders injection
//	if !stream.Type.AssignableTo(inType) {
//		panic(fmt.Errorf("sideEffect func input argument must be a underlying of %q, got underlying of %q", stream.Type, inType))
//	}
//
//	return stream.pipeline.elementWise(stream, inType, nil, f)
//
//	//return stream.pipeline.group(stream, outChannelType.Elem(), nil, func(input InputChannel, output OutputChannel) {
//	//	intermediateIn := reflect.MakeChan(inType, 0)
//	//	go func() {
//	//		defer intermediateIn.Close()
//	//		for d := range input {
//	//			intermediateIn.Send(reflect.ValueOf(d.Data))
//	//		}
//	//	}()
//	//
//	//	intermediateOut := reflect.MakeChan(outChannelType, 0)
//	//	go func() {
//	//		defer intermediateOut.Close()
//	//		fnVal.Call([]reflect.Data{intermediateIn, intermediateOut})
//	//	}()
//	//
//	//	for {
//	//		o, ok := intermediateOut.Recv()
//	//		if !ok {
//	//			return
//	//		} else {
//	//			output <- &Element{Data: o.Interface()}
//	//		}
//	//	}
//	//})
//
//}

func (stream *Stream) log(f string, args ... interface{}) {
	if stream.stage > 0 {
		log.Printf(f, args...)
	}
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
	stream.output = make(chan *Element, 1)
	var commitable Commitable
	var isCommitable bool
	if stream.fn == nil {
		//stream.isPassthrough = true
	} else {
		_, stream.isPassthrough = stream.fn.(MapFn)
		commitable, isCommitable = stream.fn.(Commitable)
	}
	if stream.isPassthrough {
		return
	}

	//start start the ack-commit accumulator that applies:
	//A. accumulation and aggregation of checkpoints when commits are expensive
	//B. back-pressure on the upstream processing when the commits are too slow given the configured buffer size

	//TODO configurable capacity for checkpoint buffers
	stream.cap = 1000000
	stream.pending = make(chan *Element, 10)
	stream.acks = make(chan Stamp, 1000)
	stream.terminate = make(chan bool, 1)
	stream.completed = make(chan bool, 1)
	commits := make(chan map[int]interface{})
	commitRequests := make(chan bool)

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
		pendingSuspendable := stream.pending
		terminated := false
		pending := Stamp {}
		pendingCheckpoints := make(map[uint32]*Checkpoint, stream.cap)
		checkpoint := make(map[int]interface{})
		pendingCommitReuqest := false

		doCommit := func() {
			if len(checkpoint) > 0 {
				pendingCommitReuqest = false
				if isCommitable {
					//stream.log("STAGE[%d] Commit Checkpoint: %v", stream.stage, checkpoint)
					commits <- checkpoint
				}
				checkpoint = make(map[int]interface{})
			}
		}

		accumulate := func() {
			for ; pending.valid() && pendingCheckpoints[pending.lo] != nil && pendingCheckpoints[pending.lo].acked; {
				lo := pending.lo
				pending.lo += 1
				chk := pendingCheckpoints[lo]
				delete(pendingCheckpoints, lo)
				checkpoint[chk.Part] = chk.Data
				//stream.log("STAGE[%d] DELETING PART %d DATA %v \n", stream.stage, chk.Part, chk.Data)
			}

			if len(pendingCheckpoints) < stream.cap && pendingSuspendable == nil {
				//release the backpressure after capacity is freed
				pendingSuspendable = stream.pending
			} else if !isCommitable {
				doCommit()
			}
		}

		maybeTerminate := func() {
			if stream.terminating {
				if len(checkpoint) == 0 {
					clean := (pendingCommitReuqest || !isCommitable) && !pending.valid()
					if clean {
						terminated = true
					}
				}
			}
			if terminated {
				//stream.log("Completed Stage %d", stream.stage)
				close(commits)
				close(commitRequests)
				close(stream.acks)
				close(stream.pending)
			} else {
				//stream.log("Terminating %d (pending commit %d) (pending acks %v)", stream.stage, checkpoint, pending)
			}
		}

		//checkpoints := func() []int64 {
		//	keys := make([]int64, 0, len(pendingCheckpoints))
		//	for k, chk := range pendingCheckpoints {
		//		v := int64(k)
		//		if chk.acked {
		//			keys = append(keys, -v)
		//		} else {
		//			keys = append(keys, v)
		//		}
		//	}
		//	return keys
		//}

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
				for u := stamp.lo; u <= stamp.hi; u ++ {
					pendingCheckpoints[u].acked = true
				}
				accumulate()
				if pendingCommitReuqest {
					doCommit()
				}
				if stream.up != nil {
					stream.up.ack(stamp)
				}
				maybeTerminate()
				//stream.log("STAGE[%d] ACK(%d) Pending: %v Checkpoints: %v\n", stream.stage, stamp, pending.String(), checkpoints())

			case e := <-pendingSuspendable:
				pending.merge(e.Stamp)
				for u := e.Stamp.lo; u <= e.Stamp.hi; u++ {
					pendingCheckpoints[u] = &e.Checkpoint
				}

				accumulate()
				maybeTerminate()
				//stream.log("STAGE[%d] Pending: %v Checkpoints: %v\n", stream.stage, pending, checkpoints())
				if len(pendingCheckpoints) == stream.cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
					pendingSuspendable = nil
					//stream.log("STAGE[%d] Applying backpressure, pending acks: %d\n", stream.stage, len(pendingCheckpoints))
				} else if len(pendingCheckpoints) > stream.cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", stream.cap))
				}

			}
		}
		stream.completed <- true
	}()

}
