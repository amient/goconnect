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

package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

type NetRoundRobin struct{}

func (n *NetRoundRobin) InType() reflect.Type {
	return goc.BinaryType
}

func (n *NetRoundRobin) OutType() reflect.Type {
	return goc.BinaryType
}

func (n *NetRoundRobin) Run(input <-chan *goc.Element, context *goc.Context) {
	receiver := context.GetReceiver()
	senders := context.MakeSenders()
	go func() {
		i := 0
		for e := range input {
			senders[i].Send(e)
			i = (i + 1) % len(senders)
		}
		for _, s := range senders {
			s.Eos()
		}
	}()

	for e := range receiver.Elements() {
		context.Emit(e)
	}

}
