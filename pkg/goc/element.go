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
	Checkpoint Checkpoint //TODO make private and make sure it never leaves the stage fn
	Value      interface{}
	Stamp 	   Stamp
	signal     ControlSignal
	ack        func(stamp Stamp)
}

func (e *Element) Ack() {
	e.ack(e.Stamp)
}

type ControlSignal uint8

const NoSignal ControlSignal = 0
const FinalCheckpoint ControlSignal = 1

type Channel chan *Element

