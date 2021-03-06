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

package io

import (
	"github.com/amient/goconnect"
	"reflect"
)

func From(list interface{}) *iterable {
	return RoundRobin(reflect.ValueOf(list).Len(), list)
}

func RoundRobin(n int, list interface{}) *iterable {
	//TODO if slice or array -> iterable
	//TODO if file or url -> file
	return &iterable{
		n:   n,
		val: reflect.ValueOf(list),
		typ: reflect.TypeOf(list),
	}
}

type iterable struct {
	n      int
	offset int
	val    reflect.Value
	typ    reflect.Type
}

func (it *iterable) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *iterable) Run(context *goconnect.Context) {
	if context.GetNodeID() == 1 {
		limit := it.val.Len()
		for l := 0; l < it.n; l++ {
			select {
			case <-context.Termination():
				return
			default:
				i := l % limit
				context.Emit(&goconnect.Element{
					Value:      it.val.Index(i).Interface(),
					Checkpoint: goconnect.Checkpoint{Data: l},
				})
			}
		}
	}
}

func (it *iterable) Commit(watermark goconnect.Watermark, ctx *goconnect.Context) error {
	it.offset = watermark[0].(int)
	//log.Println("ITERABLE COMMIT: ", watermark[0])
	return nil
}
