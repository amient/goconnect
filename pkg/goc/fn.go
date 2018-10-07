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

type Fn interface{}

type Watermark map[int]interface{}

type Root interface {
	OutType() reflect.Type
	Run(*Context)
	Commit(watermark Watermark) error
}

type Transform interface {
	InType() reflect.Type
	OutType() reflect.Type
	Run(<-chan *Element, *Context)
}

type ElementWise interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(*Element, ProcessContext)
}

type Sink interface {
	InType() reflect.Type
	Process(*Element)
	Flush() error
}

type MapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(interface{}) interface{}
}

type FilterFn interface {
	Type() reflect.Type
	Pass(value interface{}) bool
}

type FoldFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(interface{})
	Collect() Element
}

func UserMapFn(f interface{}) MapFn {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 1 {
		panic("Map function must have exactly 1 input and 1 output argument")
	}
	return &userMapFn{
		f:       reflect.ValueOf(f),
		inType:  reflect.TypeOf(f).In(0),
		outType: reflect.TypeOf(f).Out(0),
	}
}

type userMapFn struct {
	inType  reflect.Type
	outType reflect.Type
	f       reflect.Value
}

func (fn *userMapFn) InType() reflect.Type {
	return fn.inType
}

func (fn *userMapFn) OutType() reflect.Type {
	return fn.outType
}

func (fn *userMapFn) Process(input interface{}) interface{} {
	return fn.f.Call([]reflect.Value{reflect.ValueOf(input)})[0].Interface()
}

func UserFilterFn(f interface{}) FilterFn {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 1 || t.Out(0).Kind() != reflect.Bool {
		panic("Filter function must have exactly 1 input and 1 bool output argument")
	}
	return &userFilterFn{
		f: reflect.ValueOf(f),
		t: t.In(0),
	}
}

type userFilterFn struct {
	t reflect.Type
	f reflect.Value
}

func (fn *userFilterFn) Type() reflect.Type {
	return fn.t
}

func (fn *userFilterFn) Pass(input interface{}) bool {
	return fn.f.Call([]reflect.Value{reflect.ValueOf(input)})[0].Interface().(bool)
}

func UserFoldFn(initial interface{}, f interface{}) FoldFn {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func || t.NumIn() != 2 || t.NumOut() != 1 {
		panic("Fold function must have exactly 2 inputs and 0 outputs")
	}
	if t.In(0) != reflect.TypeOf(initial) {
		panic("Fold initial value must have same type as the first argument of its function")
	}

	return &userFoldFn{
		f:       reflect.ValueOf(f),
		inType:  t.In(1),
		outType: t.In(0),
		value:   reflect.ValueOf(initial),
	}
}

type userFoldFn struct {
	inType  reflect.Type
	outType reflect.Type
	f       reflect.Value
	value   reflect.Value
}

func (fn *userFoldFn) InType() reflect.Type {
	return fn.inType
}

func (fn *userFoldFn) OutType() reflect.Type {
	return fn.outType
}

func (fn *userFoldFn) Process(input interface{}) {
	fn.value = fn.f.Call([]reflect.Value{fn.value, reflect.ValueOf(input)})[0]
}

func (fn *userFoldFn) Collect() Element {
	return Element{Value: fn.value.Interface()}
}

func UserFlatMapFn(f interface{}) ElementWise {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func || t.NumIn() != 2 || t.NumOut() != 0 || t.In(1).Kind() != reflect.Chan {
		panic("FlatMap function must have 2 arguments and no return type")
	}
	return &userFlatMapFn{
		f:           reflect.ValueOf(f),
		inType:      t.In(0),
		outChanType: t.In(1),
	}
}

type userFlatMapFn struct {
	inType      reflect.Type
	outChanType reflect.Type
	f           reflect.Value
}

func (fn *userFlatMapFn) InType() reflect.Type {
	return fn.inType
}

func (fn *userFlatMapFn) OutType() reflect.Type {
	return fn.outChanType.Elem()
}

func (fn *userFlatMapFn) Process(input *Element, ctx ProcessContext) {
	inter := reflect.MakeChan(fn.outChanType, 0)
	go func() {
		fn.f.Call([]reflect.Value{reflect.ValueOf(input.Value), inter})
		inter.Close()
	}()
	for {
		if x, ok := inter.Recv(); !ok {
			break
		} else {
			ctx.Emit(x.Interface())
		}
	}
}
