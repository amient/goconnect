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

type Source struct {
	Uri           string
	Exchange      string
	ExchangeType  string
	QueueName     string
	Group         string
	BindingKey    string
	conn          *amqp.Connection
	channel       *amqp.Channel
	lastCommitTag uint64
}

func (source *Source) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (source *Source) Do(context *goc.Context) {
	var err error

	log.Printf("dialing %q", source.Uri)
	source.conn, err = amqp.Dial(source.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		<-source.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("Closing AMQP source")
	}()

	log.Printf("got Connection, getting channel")
	source.channel, err = source.conn.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got channel, declaring Exchange (%q)", source.Exchange)
	if err = source.channel.ExchangeDeclare(
		source.Exchange,     // name of the Exchange
		source.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		panic(fmt.Errorf("exchange Declare: %source", err))
	}

	log.Printf("declared Exchange, declaring Queue %q", source.QueueName)
	queue, err := source.channel.QueueDeclare(
		source.QueueName, // name of the queue
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %source", err))
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding to Exchange (key %q)", source.BindingKey)
	if err := source.channel.QueueBind(source.QueueName, source.BindingKey, source.Exchange, false, nil, ); err != nil {
		panic(err)
	}
	log.Printf("Queue bound to Exchange, starting Consume (consumer Group %q)", source.Group)

	deliveries, err := source.channel.Consume(
		source.QueueName, source.Group, false, false, false, false, nil, )

	if err != nil {
		panic(err)
	}

	for delivery := range deliveries {
		context.Emit(&goc.Element{
			Stamp:      goc.Stamp{Unix: delivery.Timestamp.Unix()},
			Checkpoint: goc.Checkpoint{Data: delivery.DeliveryTag},
			Value:      delivery.Body,
		})
	}

}

func (source *Source) Run(output chan *goc.Element) {
	var err error

	log.Printf("dialing %q", source.Uri)
	source.conn, err = amqp.Dial(source.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		<-source.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("Closing AMQP source")
	}()

	log.Printf("got Connection, getting channel")
	source.channel, err = source.conn.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got channel, declaring Exchange (%q)", source.Exchange)
	if err = source.channel.ExchangeDeclare(
		source.Exchange,     // name of the Exchange
		source.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		panic(fmt.Errorf("exchange Declare: %source", err))
	}

	log.Printf("declared Exchange, declaring Queue %q", source.QueueName)
	queue, err := source.channel.QueueDeclare(
		source.QueueName, // name of the queue
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %source", err))
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding to Exchange (key %q)", source.BindingKey)
	if err := source.channel.QueueBind(source.QueueName, source.BindingKey, source.Exchange, false, nil, ); err != nil {
		panic(err)
	}
	log.Printf("Queue bound to Exchange, starting Consume (consumer Group %q)", source.Group)

	deliveries, err := source.channel.Consume(
		source.QueueName, source.Group, false, false, false, false, nil, )

	if err != nil {
		panic(err)
	}

	for delivery := range deliveries {
		output <- &goc.Element{
			Stamp:      goc.Stamp{Unix: delivery.Timestamp.Unix()},
			Checkpoint: goc.Checkpoint{Data: delivery.DeliveryTag},
			Value:      delivery.Body,
		}
	}
}

func (source *Source) Commit(checkpoint map[int]interface{}) error {
	if checkpoint[0] != nil {
		deliverTag := checkpoint[0].(uint64)
		if err := source.channel.Ack(deliverTag, true); err != nil {
			return err
		}
		log.Printf("AMQP09 Source Committed, %d\n", deliverTag-source.lastCommitTag)
		source.lastCommitTag = deliverTag
	}
	return nil
}

func (source *Source) Close() error {

	if err := source.channel.Cancel(source.Group, true); err != nil {
		return fmt.Errorf("source cancel failed: %source", err)
	}

	if err := source.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %source", err)
	}

	return nil
}
