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

package main

import (
	"flag"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/io/amqp09"
	"github.com/amient/goconnect/io/kafka1"
)

var (
	//source arguments
	uri          = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("amqp-exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("amqp-exchange-type", "direct", "Exchange type - direct|fanout|kafkaSinkTopic|x-custom")
	queue        = flag.String("amqp-queue", "test", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("amqp-key", "", "AMQP binding key")
	consumerTag  = flag.String("amqp-consumer-tag", "test-consumer", "AMQP consumer tag (should not be blank)")

	//sink arguments
	kafkaSinkBootstrap = flag.String("kafka.bootstrap", "localhost:9092", "Kafka Bootstrap servers")
	kafkaSinkTopic     = flag.String("kafka.kafkaSinkTopic", "test", "Target Kafka Topic")

)

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	amqp09.DeclareExchange(*uri, *exchange, *exchangeType, *queue)

	messages := pipeline.Root(&amqp09.Source{
		Uri:         *uri,
		Exchange:    *exchange,
		QueueName:   *queue,
		ConsumerTag: *consumerTag,
		BindingKey:  *bindingKey,
		PrefetchCount: 2000,
	}).Buffer(100000)

	messages.Apply(&kafka1.Sink{
		Topic: *kafkaSinkTopic,
		ProducerConfig: kafka1.ConfigMap{
			"bootstrap.servers": *kafkaSinkBootstrap,
			"linger.ms":         100,
			"message.max.bytes": 200000000,
			"compression.type":  "gzip"}})

	pipeline.Run()

}
