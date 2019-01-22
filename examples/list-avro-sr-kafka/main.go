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
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)
import avrolib "github.com/amient/avro"

var (
	kafkaBootstrap    = flag.String("kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaTopic        = flag.String("kafka-topic", "test-avro", "Destination Kafka Topic")
	kafkaUsername     = flag.String("username", "", "Destination Kafka Principal")
	kafkaPassword     = flag.String("password", "", "Destination Kafka Password")
	schemaRegistryUrl = flag.String("schema-registry-url", "http://localhost:8082", "Destination Schema Registry")

	schema, err = avrolib.ParseSchema(`{
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

type Example struct {
	seqNo     uint64
	timestamp uint64
}

func main() {

	flag.Parse()

	pipeline := goc.NewPipeline().WithCoders(coder.Registry())

	r1 := avrolib.NewGenericRecord(schema)
	r1.Set("seqNo", int64(1000))
	r1.Set("timestamp", int64(19834720000))

	r2 := avrolib.NewGenericRecord(schema)
	r2.Set("seqNo", int64(2000))
	r2.Set("timestamp", int64(19834723000))

	pipeline.
		Root(io.RoundRobin(10, []*avrolib.GenericRecord{r1, r2})).Buffer(5000).
		Apply(new(avro.GenericEncoder)).
		//Apply(new(std.Out)).TriggerEach(1)
		Apply(&avro.SchemaRegistryEncoder{Url: *schemaRegistryUrl, Subject: *kafkaTopic + "-value"}).
		Apply(&kafka1.Sink{
			Topic: *kafkaTopic,
			ProducerConfig: kafka.ConfigMap{
				"bootstrap.servers": *kafkaBootstrap,
				"security.protocol": "SASL_SSL",
				"sasl.mechanisms":   "PLAIN",
				"sasl.username":     *kafkaUsername,
				"sasl.password":     *kafkaPassword,
				"linger.ms":         50,
				"compression.type":  "snappy",
			}})
	pipeline.Run()

}