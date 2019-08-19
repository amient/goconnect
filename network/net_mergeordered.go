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
	"github.com/amient/goconnect"
	"reflect"
)

type NetMergeOrdered struct{}

func (n *NetMergeOrdered) InType() reflect.Type {
	return goc.BinaryType
}

func (n *NetMergeOrdered) OutType() reflect.Type {
	return goc.BinaryType
}

func (n *NetMergeOrdered) Run(input <-chan *goc.Element, context *goc.Context) {

	LastNode := context.GetNumPeers()
	mergeOnThisNode := context.GetNodeID() == LastNode
	var recv goc.Receiver
	if mergeOnThisNode {
		recv = context.GetReceiver()
	}
	send := context.MakeSender(LastNode)

	go func() {
		for e := range input {
			send.Send(e)
		}
		send.Eos()
	}()

	if mergeOnThisNode {
		buf := goc.NewOrderedElementSet(10)
		for e := range recv.Elements() {
			buf.AddElement(e, context)
		}
	}

}
