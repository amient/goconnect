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

type Root interface {
	OutType() reflect.Type
	Do(*Context)
}

type Transform interface {
	InType() reflect.Type
	OutType() reflect.Type
	Run(<-chan *Element, *Context)
}

type ElementWiseFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(*Element, *Context)
}

type ForEach interface {
	InType() reflect.Type
	Run(<-chan *Element, *Context)
}

type ForEachFn interface {
	InType() reflect.Type
	Process(input *Element)
}

type MapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) *Element
}

type FilterFn interface {
	Type() reflect.Type
	Pass(input *Element) bool
}

type FlatMapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) []*Element
}

type GroupFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element)
	Trigger() []*Element
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

func (fn *userMapFn) Process(input *Element) *Element {
	return &Element{
		Stamp: input.Stamp,
		Value: fn.f.Call([]reflect.Value{reflect.ValueOf(input.Value)})[0].Interface(),
	}
}

func UserFilterFn(f interface{}) FilterFn {
	t := reflect.TypeOf(f)
	if t.Kind() != reflect.Func || t.NumIn() != 1 || t.NumOut() != 1 || t.Out(0).Kind() != reflect.Bool{
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

func (fn *userFilterFn) Pass(input *Element) bool {
	return fn.f.Call([]reflect.Value{reflect.ValueOf(input.Value)})[0].Interface().(bool)
}