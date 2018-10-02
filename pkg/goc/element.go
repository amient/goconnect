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

type Element struct {
	Checkpoint Checkpoint //TODO make private and make sure it never leaves the stage Fn
	Value      interface{}
	Stamp 	   Stamp
	FromNodeId uint16
	ack        func(uniq uint64)
}

func (e *Element) Ack() {
	e.ack(e.Stamp.Uniq)
}

func NewOrderedElementSet(cap int) *OrderedElementSet {
	return &OrderedElementSet{
		elements: make(map[uint64]*Element, cap),
	}
}

type OrderedElementSet struct {
	next     uint64
	elements map[uint64]*Element
}

func (set *OrderedElementSet) AddElement(elementToAdd *Element, context *Context) {
	//FIXME ordered element set must have its own stamp
	set.elements[elementToAdd.Stamp.Uniq] = elementToAdd
	for ; set.elements[set.next + 1] != nil; {
		set.next ++
		context.Emit(set.elements[set.next])
		delete(set.elements, set.next)
	}
}






