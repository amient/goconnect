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
	"math/rand"
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

func RandomOf(n int, list interface{}) goc.RootFn {
	//TODO validate array or slice
	return &randomOf{
		n:   n,
		val: reflect.ValueOf(list),
		typ: reflect.TypeOf(list),
	}
}

type iterable struct {
	offset int
	n      int
	val    reflect.Value
	typ    reflect.Type
}

func (it *iterable) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *iterable) Run(output chan *goc.Element) {
	limit := it.val.Len()
	for l := 0; l < it.n; l++ {
		i := l % limit
		output <- &goc.Element{
			Checkpoint: goc.Checkpoint{Data: l},
			Value:      it.val.Index(i).Interface(),
		}
	}
	close(output)
}

func (it *iterable) Do(collector *goc.Collector) {
	if collector.NodeID == 1 {
		limit := it.val.Len()
		for l := 0; l < it.n; l++ {
			i := l % limit
			collector.Emit2(it.val.Index(i).Interface(), goc.Checkpoint{Data: l})
		}
	}
}

func (it *iterable) Commit(checkpoint map[int]interface{}) error {
	log.Println("DEBUG ITERABLE COMMIT UP TO", checkpoint[0])
	it.offset = checkpoint[0].(int)
	return nil
}

type randomOf struct {
	n   int
	val reflect.Value
	typ reflect.Type
}

func (it *randomOf) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *randomOf) Run(output chan *goc.Element) {
	size := it.val.Len()
	for l := 0; l < it.n; l++ {
		i := rand.Int() % size
		output <- &goc.Element{
			Checkpoint: goc.Checkpoint{Data: i},
			Value:      it.val.Index(i).Interface(),
		}
	}
	close(output)
}
