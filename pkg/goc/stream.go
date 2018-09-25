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
	"reflect"
)

type Stream struct {
	Type           reflect.Type
	Fn             Fn
	Id             int
	Up             *Stream
	pipeline       *Pipeline
}

func (stream *Stream) Apply(f Fn) *Stream {
	return stream.pipeline.Apply(stream, f)
}

func (stream *Stream) Map(f interface{}) *Stream {
	return stream.pipeline.Map(stream, UserMapFn(f))
}

func (stream *Stream) Filter(f interface{}) *Stream {
	return stream.pipeline.Filter(stream, UserFilterFn(f))
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
