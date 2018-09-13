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
	"sync"
)

type Stream struct {
	Type         reflect.Type
	fn           Fn
	up           *Stream
	pipeline     *Pipeline
	stage        int
	output       OutputChannel
	runner       func(output OutputChannel)
	termination  chan bool
	closed       bool
	_cap         int
	_lock        sync.RWMutex
	_pending     map[int][]Stamp
	_checkpoints map[Stamp]interface{}
	_acked       map[Stamp]bool
}

func (stream *Stream) pending(element *Element) {
	stream._lock.Lock()
	defer stream._lock.Unlock()
	part := element.Checkpoint.Part
	var ok bool
	if _, ok = stream._pending[part]; !ok {
		stream._pending[part] = make([]Stamp, 0, stream._cap)
	}
	stream._pending[part] = append(stream._pending[part], element.Stamp)
	stream._checkpoints[element.Stamp] = element.Checkpoint.Data
	//log.Printf("STAGE[%d] PENDING(%d) pending: %v acked:%d \n", stream.stage, element.Stamp, stream._pending, stream.acked())
}

func (stream *Stream) acked() []Stamp {
	acked := make([]Stamp, 0, len(stream._acked))
	for k := range stream._acked {
		acked = append(acked, k)
	}
	return acked
}
func (stream *Stream) clean() bool {
	clean := true
	for _, p := range stream._pending {
		clean = clean && len(p) == 0
	}
	return clean
}


func (stream *Stream) ack(stamps ... Stamp) error {
	stream._lock.Lock()
	defer stream._lock.Unlock()
	for _, stamp := range stamps {
		stream._acked[stamp] = true
	}
	upStamps := make([]Stamp, 0, len(stamps))
	chk := make(map[int]interface{}, len(stream._pending))

	for part, pending := range stream._pending {
		var upto Stamp
		for ; len(pending) > 0 && stream._acked[pending[0]]; {
			s := pending[0]
			if s > upto {
				upto = s
				chk[part] = stream._checkpoints[upto]
			}
			delete(stream._acked, s)
			delete(stream._checkpoints, s)
			upStamps = append(upStamps, s)
			pending = pending[1:]
		}
		stream._pending[part] = pending
	}

	//log.Printf("STAGE[%d] ACK(%d) pending: %v acked:%d \n", stream.stage, stamps, stream._pending, stream.acked())
	if commitable, ok := stream.fn.(Commitable); ok {
		if err := commitable.Commit(chk); err != nil {
			return err
		}
	}

	if stream.closed && stream.clean() {
		//log.Printf("STAGE[%d] ACK(%d) clean: %v pending: %v\n", stream.stage, stamps, stream.clean(), stream._pending)
		stream.termination <- true
	}

	if len(upStamps) > 0 {
		if stream.up != nil {
			return stream.up.ack(upStamps...)
		}
	}

	return nil
}

func (stream *Stream) close() {
	if ! stream.closed {
		stream.closed = true
		log.Println("Closing Stage", stream.stage, stream.Type)
		close(stream.output)
		if fn, ok := stream.fn.(Closeable); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}

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
