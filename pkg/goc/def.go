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

type Def struct {
	Type                   reflect.Type
	Fn                     Fn
	Id                     int
	Up                     *Def
	pipeline               *Pipeline
	maxVerticalParallelism int
}

func (def *Def) Apply(f Fn) *Def {
	return def.pipeline.Apply(def, f)
}

func (def *Def) Map(f interface{}) *Def {
	return def.pipeline.Map(def, UserMapFn(f))
}

func (def *Def) Filter(f interface{}) *Def {
	return def.pipeline.Filter(def, UserFilterFn(f))
}

func (def *Def) MaxVerticalParallelism(i int) *Def {
	def.maxVerticalParallelism = i
	return def
}

//func (def *Def) Group(f func(element *Element, output Elements)) *Def {
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
//	if !def.Type.AssignableTo(inType) {
//		panic(fmt.Errorf("sideEffect func input argument must be a underlying of %q, got underlying of %q", def.Type, inType))
//	}
//
//	return def.pipeline.elementWise(def, inType, nil, f)
//
//	//return def.pipeline.group(def, outChannelType.Elem(), nil, func(input InputChannel, output Elements) {
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
