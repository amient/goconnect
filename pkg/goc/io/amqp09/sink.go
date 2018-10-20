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

package amqp09

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/streadway/amqp"
	"log"
	"reflect"
)

type Sink struct {
	Uri        string
	Exchange   string
	BindingKey string
}

func (s *Sink) InType() reflect.Type {
	return goc.BinaryType
}

func (s *Sink) Process(element *goc.Element, ctx *goc.Context) {
	var err error
	var conn *amqp.Connection
	var channel *amqp.Channel

	if ctx.Get(0) == nil {
		log.Printf("dialing %q", s.Uri)
		conn, err = amqp.Dial(s.Uri)
		if err != nil {
			panic(err)
		}
		ctx.Put(0, conn)
	} else {
		conn = ctx.Get(0).(*amqp.Connection)
	}

	if ctx.Get(1) == nil {
		channel, err = conn.Channel()
		if err != nil {
			panic(err)
		}
		ctx.Put(1, channel)
		log.Printf("Got Channel bound to Exchange")
		//confirmations := make(chan amqp.Confirmation)
		//channel.NotifyPublish(confirmations)
		//doClose := make(chan *amqp.Error)
		//channel.NotifyClose(doClose)
		//counter := uint64(0)
		//ctx.Put(2, &counter)
		//go func() {
		//	for {
		//		select {
		//		case <-doClose:
		//			return
		//		case _, ok := <-confirmations:
		//			if !ok {
		//				return
		//			}
		//		}
		//	}
		//}()
		//channel.Confirm(false)
	} else {
		channel = ctx.Get(1).(*amqp.Channel)
	}

	channel.Publish(s.Exchange, s.BindingKey, false, false, amqp.Publishing{
		ContentType:  "application/octet-stream",
		Body:         element.Value.([]byte),
		DeliveryMode: amqp.Persistent,
		Priority:     0,
	})

	//FIXME use confirmations to provide guarantees, check .Channel() for details about the deliveryTag
	element.Ack()

}

func (s *Sink) Flush(ctx *goc.Context) error {
	//TODO wait for all confirmations to complete
	return nil
}

func (s *Sink) Close(ctx *goc.Context) error {
	conn := ctx.Get(0).(*amqp.Connection)
	channel := ctx.Get(1).(*amqp.Channel)
	if err := channel.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %source", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %source", err)
	}

	return nil
}
