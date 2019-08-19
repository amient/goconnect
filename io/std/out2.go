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

package std

import (
	"bufio"
	"github.com/amient/goconnect"
	"log"
	"os"
	"reflect"
	"time"
)

type Out2 struct {}

func (sink *Out2) InType() reflect.Type {
	return goconnect.AnyType
}

//this type of implementation doesn't work well because it doesn't have a way of hooking into the termination condition
//TODO in other words we need a processor that takes a channel input which can be exhausted
//TODO also the meaning of .TriggerEach and .TriggerEvery is not clear
func (sink *Out2) Materialize() func(input *goconnect.Element, context goconnect.PContext) {

	processed := make(chan *goconnect.Element)

	go func() {
		defer log.Println("!!!")
		stdout := bufio.NewWriter(os.Stdout)
		buffer := make([]*goconnect.Element, 0, 100)
		flush := func() {
			if len(buffer) > 0 {
				if err := stdout.Flush(); err != nil {
					panic(err)
				}
				for _, e := range buffer {
					e.Ack()
				}
				buffer = make([]*goconnect.Element, 0, 100)
			}
		}
		t := time.NewTicker(50 * time.Millisecond).C
		for {
			select {
				case <- t:
					flush()
				case e, ok := <- processed:
					if !ok {
						flush()
						return
					} else {
						process(stdout, e.Value)
						buffer = append(buffer, e)
						if len(buffer) == cap(buffer) {
							flush()
						}
					}

			}
		}
	}()

	return func(input *goconnect.Element, context goconnect.PContext) {
		processed <- input
	}
}
