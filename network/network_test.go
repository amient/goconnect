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
	"log"
	"reflect"
	"testing"
	"time"
)

func TestNetworkTools(t *testing.T) {

	server := NewServer("127.0.0.1:10000")
	handler := server.NewReceiver(1)
	sender := newSender("127.0.0.1:10000", handler.id, 1)

	server.Start()
	sender.Start()

	fixture := goc.Element{
		Stamp: goc.Stamp{
			Unix: time.Now().Unix(),
			Uniq: 1,
		},
		Value: []byte("Hello World"),
	}

	sender.Send(&fixture)

	received := <-handler.Elements()
	if (*received).Stamp.Uniq != fixture.Stamp.Uniq {
		log.Println(fixture.Stamp)
		log.Println((*received).Stamp)
		panic("Are not equal")
	}

	if !reflect.DeepEqual((*received).Value, fixture.Value) {
		log.Println(fixture.Value)
		log.Println((*received).Value)
		panic("Are not equal")
	}

	server.Close()
	sender.Close()
}
