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
	"github.com/amient/goconnect/io/kafka1"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	kafkaSourceBootstrap = flag.String("source-bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaSourceGroup     = flag.String("source-group", "goconnect-mirror", "Source Kafka Consumer Group")
	kafkaSourceTopic     = flag.String("source-topic", "test", "Source Kafka Topic")
	//
	kafkaUsername = flag.String("username", "", "Kafka Principal")
	kafkaPassword = flag.String("password", "", "Kafka Principal Password")
	//
	kafkaSinkBootstrap = flag.String("sink-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaSinkTopic     = flag.String("sink-topic", "test-copy", "Destination Kafka Topic")
)

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	source := pipeline.Root(&kafka1.Source{
		Topic: *kafkaSourceTopic,
		ConsumerConfig: kafka1.ConfigMap{
			"bootstrap.servers":       *kafkaSourceBootstrap,
			"group.id":                *kafkaSourceGroup,
			"auto.offset.reset":       "earliest",
			"security.protocol":       "SASL_SSL",
			"fetch.message.max.bytes": 10000,
			"sasl.mechanisms":         "PLAIN",
			"sasl.username":           *kafkaUsername,
			"sasl.password":           *kafkaPassword,
		}}).Buffer(100000)

	source.Apply(&kafka1.Sink{
		Topic: *kafkaSinkTopic,
		ProducerConfig: kafka.ConfigMap{
			"bootstrap.servers": *kafkaSinkBootstrap,
			"security.protocol": "SASL_SSL",
			"sasl.mechanisms":   "PLAIN",
			"sasl.username":     *kafkaUsername,
			"sasl.password":     *kafkaPassword,
			"compression.type":  "snappy",
			"linger.ms":         10}})

	pipeline.Run()

}
