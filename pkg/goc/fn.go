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

import "reflect"

type Fn interface {}

type RootFn interface {
	OutType() reflect.Type
	Run(output chan *Element) //deprecated
	Do(*Context)
}

type TransformFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Run(<-chan *Element, *Context) //deprecated
}

type ElementWiseFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(*Element, *Context)
}

type MapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) *Element
}

type ForEachFn interface {
	InType() reflect.Type
	Process(input *Element)
}

type FlatMapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) []*Element //deprecated
}

type GroupFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element)
	Trigger() []*Element
}


type Commitable interface {
	Commit(checkpoint map[int]interface{}) error
}


