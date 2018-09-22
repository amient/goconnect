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
	"io"
	"log"
	"reflect"
)

type Stream struct {
	Type           reflect.Type
	Fn             Fn
	Id             int
	Up             *Stream
	pipeline       *Pipeline
	stage          int
	output         chan *Element
	runner         func(output chan *Element)
	isPassthrough  bool
	pending        chan *Element
	acks           chan *Stamp
	terminate      chan bool
	terminating    bool
	completed      chan bool
	closed         bool
	cap            int
	highestPending uint64
	highestAcked   uint64
}

func (stream *Stream) Apply(f Fn) *Stream {
	return stream.pipeline.Apply(stream, f)
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

	return stream.pipeline.elementWise(stream, fnType.Out(0), nil, func(input *Element, output chan *Element) {
		v := reflect.ValueOf(input.Value)
		r := fnVal.Call([]reflect.Value{v})
		output <- &Element{
			Value: r[0].Interface(),
			Stamp: input.Stamp,
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
	return stream.pipeline.elementWise(stream, fnType.In(0), nil, func(input *Element, output chan *Element) {
		v := reflect.ValueOf(input.Value)
		stamp = stamp.Merge(input.Stamp)
		if fnVal.Call([]reflect.Value{v})[0].Bool() {
			output <- &Element{
				Stamp:      stamp.Merge(input.Stamp),
				Value:      input.Value,
				Checkpoint: input.Checkpoint,
			}
			stamp = Stamp{}
		} else {
			input.Ack()
		}
	})

}

//func (stream *Stream) Group(f func(element *Element, output Elements)) *Stream {
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
//	//return stream.pipeline.group(stream, outChannelType.Elem(), nil, func(input InputChannel, output Elements) {
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
	element.ack = stream.ack
	if element.Stamp.Hi > stream.highestPending {
		stream.highestPending = element.Stamp.Hi
	}
	if !stream.isPassthrough {
		//stream.log("STAGE[%d] Incoming %d", stream.stage, element.Stamp)
		stream.pending <- element
	}
}

func (stream *Stream) ack(s *Stamp) {
	if stream.isPassthrough {
		stream.Up.ack(s)
	} else {
		stream.acks <- s
	}
}

func (stream *Stream) close() {
	if ! stream.closed {
		stream.log("STAGE[%d] Closed %v", stream.stage, stream.Type)
		if fn, ok := stream.Fn.(io.Closer); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}
		stream.closed = true
	}
}

func (stream *Stream) initialize(stage int) {
	stream.stage = stage
	var commitable Commitable
	var isCommitable bool
	if stream.Fn == nil {
		stream.isPassthrough = true
	} else {
		_, stream.isPassthrough = stream.Fn.(MapFn)
		commitable, isCommitable = stream.Fn.(Commitable)
	}

	if stream.isPassthrough {
		log.Printf("Initializing Passthru Stage %d %v\n", stage, stream.Type)
		return
	} else {
		log.Printf("Initializing Buffered Stage %d %v\n", stage, stream.Type)
	}

	//start start the ack-commit accumulator that applies:
	//A. accumulation and aggregation of checkpoints when commits are expensive
	//B. back-pressure on the input processing when the commits are too slow given the configured buffer size

	//TODO configurable capacity for checkpoint buffers
	stream.cap = 100000 //FIXME if this buffer is smaller then any aggregation stage limit the pipeline hangs:
	// the question is whether this is ok and should be simply configured accordingly
	// or should there be some kind of high-level back-pressure where the aggregations are "forced" to emit

	stream.pending = make(chan *Element, 1000) //TODO this buffer is important so make it configurable but must be >= stream.cap
	stream.acks = make(chan *Stamp, 10000)
	stream.terminate = make(chan bool, 2)
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
		pending := Stamp{}
		pendingCheckpoints := make(map[uint64]*Checkpoint, stream.cap)
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

		maybeTerminate := func() {
			if stream.terminating {
				if len(checkpoint) == 0 && stream.highestPending == stream.highestAcked {
					clean := (pendingCommitReuqest || !isCommitable) && !pending.Valid()
					if clean {
						terminated = true
						stream.log("STAGE[%d] Completed", stream.stage)
						close(commits)
						close(commitRequests)
						commitRequests = nil
						close(stream.acks)
						close(stream.pending)
						stream.acks = nil
						stream.pending = nil
					}
				}
			}
		}

		resolveAcks := func() {
			//resolve acks <> pending
			for ; pending.Valid() && pendingCheckpoints[pending.Lo] != nil && pendingCheckpoints[pending.Lo].acked; {
				lo := pending.Lo
				pending.Lo += 1
				chk := pendingCheckpoints[lo]
				delete(pendingCheckpoints, lo)
				checkpoint[chk.Part] = chk.Data
				//stream.log("STAGE[%d] DELETING PART %d DATA %v \n", stream.stage, chk.Part, chk.Data)
			}
			if len(pendingCheckpoints) < stream.cap && pendingSuspendable == nil {
				//release the backpressure after capacity is freed
				pendingSuspendable = stream.pending
			}

			//commit if pending commit request or not commitable in which case commit requests are never fired
			if pendingCommitReuqest || !isCommitable {
				doCommit()
			}
			maybeTerminate()
		}

		for !terminated {
			select {
			case <-stream.terminate:
				stream.log("STAGE[%d] Terminating", stream.stage)
				stream.terminating = true
				maybeTerminate()
			case <-commitRequests:
				pendingCommitReuqest = true
				doCommit()
				maybeTerminate()
			case stamp := <-stream.acks:
				if stamp.Hi > stream.highestAcked {
					stream.highestAcked = stamp.Hi
				}
				//this doesn't have to block because it doesn't create any memory build-Up, if anything it frees memory
				for u := stamp.Lo; u <= stamp.Hi; u ++ {
					if c, ok := pendingCheckpoints[u]; ok {
						c.acked = true
					}
				}

				resolveAcks()

				//stream.log("STAGE[%d] ACK(%d) Pending: %v Checkpoints: %v\n", stream.stage, stamp, pending.String(), pendingCheckpoints)
				if stream.Up != nil {
					stream.Up.ack(stamp)
				}

			case e := <-pendingSuspendable:
				if terminated {
					panic(fmt.Errorf("STAGE[%d] Illegal Pending %v", stream.stage, e))
				}
				pending.Merge(e.Stamp)
				for u := e.Stamp.Lo; u <= e.Stamp.Hi; u++ {
					pendingCheckpoints[u] = &e.Checkpoint
				}

				resolveAcks()

				//stream.log("STAGE[%d] Pending: %v Checkpoints: %v\n", stream.stage, pending, len(pendingCheckpoints))
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
