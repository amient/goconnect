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
	"reflect"
)

type Stream struct {
	Type       reflect.Type
	output     OutputChannel
	runner     func(output OutputChannel)
	pipeline   *Pipeline
	closed     bool
	checkpoint Checkpoint
	fn         Fn
}

func (stream *Stream) close() {
	if ! stream.closed {
		close(stream.output)
		stream.closed = true
	}
}

func (stream *Stream) commit() bool {
	if fn, ok := stream.fn.(Commitable); ok {
		if err := fn.Commit(stream.checkpoint); err != nil {
			panic(err)
		}
		return true
	} else {
		return false
	}
}

func (stream *Stream) Apply(f Fn) *Stream {
	switch fn := f.(type) {
	case FlatMapFn:
		return stream.pipeline.FlatMap(stream, fn)
	case MapFn:
		return stream.pipeline.Map(stream, fn)
	case ForEachFn:
		return stream.pipeline.ForEach(stream, fn)
		//case TransformFn:
		//	return stream.pipeline.Transform(stream, fn)
	default:
		panic(fmt.Errorf("reflective transforms need: a) stream.Transform to have guaranteees and b) perf-tested"))
		//if method, exists := reflect.TypeOf(f).MethodByName("Fn"); !exists {
		//	panic(fmt.Errorf("transform must provide Fn method"))
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
		//	fn := reflect.MakeFunc(reflect.FuncOf(args, ret, false), func(args []reflect.Value) (results []reflect.Value) {
		//		methodArgs := append([]reflect.Value{v}, args...)
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

	//TODO this check will deffered on after network and type coders injection
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
//				intermediateIn.Send(reflect.ValueOf(d.Value))
//			}
//		}()
//
//		intermediateOut := reflect.MakeChan(outChannelType, 0)
//		go func() {
//			defer intermediateOut.Close()
//			fnVal.Call([]reflect.Value{intermediateIn, intermediateOut})
//		}()
//
//		for {
//			o, ok := intermediateOut.Recv()
//			if !ok {
//				return
//			} else {
//				output <- &Element{Value: o.Interface()}
//			}
//		}
//	})
//
//}
