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
	avrolib "github.com/amient/avro"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/coder/serde"
	"github.com/amient/goconnect/io/kafka1"
	"github.com/amient/goconnect/io/std"
)

var (
	kafkaBootstrap    = flag.String("kafka-bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaGroup        = flag.String("kafka-group", "goc-avro-poc", "Source Kafka Consumer Group")
	kafkaTopic        = flag.String("kafka-topic", "", "Source Kafka Topic")
	schemaRegistryUrl = flag.String("schema-registry-url", "http://localhost:8081", "Schema Registry URL")
	targetSchema, err = avrolib.ParseSchema(`{
  "type": "record",
  "name": "Example",
  "fields": [
    {
      "name": "seqNo",
      "type": "long",
      "default": 0
    },
    {
      "name": "timestamp",
      "type": "long",
      "default": -1
    }
  ]}`)
)


func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	consumerConfig := kafka1.ConfigMap{"bootstrap.servers": *kafkaBootstrap, "group.id": *kafkaGroup}

	pipeline.Root(&kafka1.Source{*kafkaTopic, consumerConfig}).
		Apply(&serde.SchemaRegistryDecoder{Url: *schemaRegistryUrl}).
		Apply(&serde.GenericProjector{targetSchema }).
		Apply(new(serde.JsonEncoder)).
		Apply(new(std.Out)).TriggerEach(1)
	//TODO Apply(&aws.SqsSink{"..."})

	pipeline.Run()
}
