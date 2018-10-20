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
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder"
	"github.com/amient/goconnect/pkg/goc/coder/avro"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	kafkaSourceBootstrap = flag.String("kafka.source.bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaSourceGroup     = flag.String("kafka.source.group", "goc-avro-poc", "Source Kafka Consumer Group")
	kafkaSourceTopic     = flag.String("kafka.source.topic", "_metrics", "Source Kafka Topic")
	schemaRegistryUrl    = flag.String("schema.registry.url", "http://localhost:8081", "Schema Registry URL")
)

func main() {

	pipeline := goc.NewPipeline().WithCoders(coder.Registry())

	consumerConfig := kafka.ConfigMap{"bootstrap.servers": *kafkaSourceBootstrap, "group.id": *kafkaSourceGroup}

	pipeline.Root(&kafka1.Source{*kafkaSourceTopic, consumerConfig}).
		//TODO Apply(&ConfluentSchemaRegistry{*schemaRegistryUrl}).
		Apply(new(avro.GenericDecoder)).
		//TODO Apply(new(avro.JsonEncoder)).
		Apply(new(std.Out)).TriggerEach(1)
		//TODO Apply(&aws.SqsSink{"..."})

	pipeline.Run()
}
