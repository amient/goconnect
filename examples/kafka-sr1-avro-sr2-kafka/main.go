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
	"github.com/amient/goconnect/coder/avro"
	"github.com/amient/goconnect/io/kafka1"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	avrolib "github.com/amient/avro"
)

var (
	sourceKafkaBootstrap    = flag.String("source-kafka-bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	sourceKafkaGroup        = flag.String("source-kafka-group", "goc-avro-poc", "Source Kafka Consumer Group")
	sourceKafkaTopic        = flag.String("source-kafka-topic", "avro", "Source Kafka Topic")
	soureKafkaUsername      = flag.String("source-username", "", "Source Kafka Principal")
	soureKafkaPassword      = flag.String("source-password", "", "Source Kafka Password")
	sourceSchemaRegistryUrl = flag.String("source-schema-registry-url", "http://localhost:8081", "Source Kafka Topic")
	//
	sinkKafkaBootstrap    = flag.String("sink-kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	sinkKafkaTopic        = flag.String("sink-kafka-topic", "avro-copy", "Destination Kafka Topic")
	sinkKafkaUsername     = flag.String("sink-username", "", "Source Kafka Principal")
	sinkKafkaPassword     = flag.String("sink-password", "", "Source Kafka Password")
	sinkSchemaRegistryUrl = flag.String("sink-schema-registry-url", "http://localhost:8081", "Source Kafka Topic")
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

	pipeline := goc.NewPipeline().WithCoders(coder.Registry())

	pipeline.
		Root(&kafka1.Source{
			Topic: *sourceKafkaTopic,
			ConsumerConfig: kafka.ConfigMap{
				"bootstrap.servers": *sourceKafkaBootstrap,
				"group.id":          *sourceKafkaGroup,
				"sasl.username":     *soureKafkaUsername,
				"sasl.password":     *soureKafkaPassword,
				"auto.offset.reset": "earliest"}}).Buffer(10000).

		Apply(&avro.SchemaRegistryDecoder{Url: *sourceSchemaRegistryUrl}).

		Apply(&avro.GenericProjector{targetSchema }).

		Apply(new(avro.GenericEncoder)).

		Apply(&avro.SchemaRegistryEncoder{Url: *sinkSchemaRegistryUrl, Subject: *sinkKafkaTopic + "-value"}).

		Apply(&kafka1.Sink{
			Topic: *sinkKafkaTopic,
			ProducerConfig: kafka.ConfigMap{
				"bootstrap.servers": *sinkKafkaBootstrap,
				"sasl.username":     *sinkKafkaUsername,
				"sasl.password":     *sinkKafkaPassword,
				"linger.ms":         100}})

	pipeline.Run()

}
