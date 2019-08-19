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
	"github.com/amient/goconnect"
	"github.com/streadway/amqp"
	"log"
	"reflect"
	"time"
)

type Source struct {
	Uri           string
	Exchange      string
	QueueName     string
	ConsumerTag   string
	BindingKey    string
	PrefetchCount int
	PrefetchSize  int
}

func (source *Source) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (source *Source) Run(context *goconnect.Context) {
	var err error

	log.Printf("AMQP dialing %q ..", source.Uri)
	conn, err := amqp.Dial(source.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		err := <-conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("AMQP Closing: %v", err)
	}()

	log.Printf("AMQP got connection, getting channel..")
	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	if source.BindingKey != "" {
		log.Printf("AMQP binding to Exchange (key %q)", source.BindingKey)
		if err := channel.QueueBind(source.QueueName, source.BindingKey, source.Exchange, false, nil, ); err != nil {
			panic(err)
		}
	}

	log.Printf("AMQP bound to queue %q via exchange %q", source.QueueName, source.Exchange)

	context.Put(0, conn)
	context.Put(1, channel)
	context.Put(2, uint64(0))   //lastCommitTag
	context.Put(3, time.Time{}) //lastCommitTime

	channel.Qos(source.PrefetchCount, source.PrefetchSize, false)

	deliveries, err := channel.Consume(
		source.QueueName, source.ConsumerTag, false, false, false, false, nil)

	if err != nil {
		panic(err)
	}

	log.Printf("AMQP Consumer starting with tag: %q", source.ConsumerTag)

	for {
		select {
		case <-context.Termination():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			context.Emit(&goconnect.Element{
				Stamp:      goconnect.Stamp{Unix: delivery.Timestamp.Unix()},
				Checkpoint: goconnect.Checkpoint{Data: delivery.DeliveryTag},
				Value:      delivery.Body,
			})
		}
	}
}

func (source *Source) Commit(checkpoint goconnect.Watermark, ctx *goconnect.Context) error {
	channel := ctx.Get(1).(*amqp.Channel)
	lastCommitTag := ctx.Get(2).(uint64)
	lastCommitTime := ctx.Get(3).(time.Time)
	if checkpoint[0] != nil {
		deliverTag := checkpoint[0].(uint64)
		//TODO test that RabbitMQ honors the acks up to the given devlieryTag and not like JMS (everything consumer so far)
		if err := channel.Ack(deliverTag, true); err != nil {
			return err
		}
		now := time.Now()
		diff := now.Sub(lastCommitTime)
		if diff > 10*time.Second {
			numCommitted := deliverTag - lastCommitTag
			ctx.Put(3, now)
			log.Printf("AMQP09 Source Ack TPS: %d\n", numCommitted / 10)
			ctx.Put(2, deliverTag)
		}
	}
	return nil
}

func (source *Source) Close(ctx *goconnect.Context) error {
	conn := ctx.Get(0).(*amqp.Connection)
	channel := ctx.Get(1).(*amqp.Channel)

	if err := channel.Cancel(source.ConsumerTag, true); err != nil {

		return fmt.Errorf("source cancel failed: %source", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %source", err)
	}

	return nil
}
