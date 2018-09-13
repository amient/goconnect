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
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
)

func Iterable(list interface{}) goc.RootFn {
	//TODO validate array or slice
	return &iterable{
		val: reflect.ValueOf(list),
		typ: reflect.TypeOf(list),
	}
}

type iterable struct {
	val reflect.Value
	typ reflect.Type
}

func (it *iterable) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *iterable) Run(output goc.OutputChannel) {
	for i := 0; i < it.val.Len(); i++ {
		output <- &goc.Element{
			Checkpoint: goc.Checkpoint{Data: i},
			Value:      it.val.Index(i).Interface(),
		}
	}
}

func (it *iterable) Commit(checkpoint map[int]interface{}) error {
	log.Println("ACK-UP-TO", checkpoint)
	return nil
}
