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
	"github.com/amient/goconnect/coder/avro"
	"github.com/amient/goconnect/io"
	"github.com/amient/goconnect/io/kafka1"
	"github.com/amient/goconnect/io/std"
	"time"
)


var (
	kafkaBootstrap    = flag.String("kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaCaCert       = flag.String("kafka-ca-cert", "", "Destination Kafka CA Certificate")
	kafkaTopic        = flag.String("kafka-topic", "test_avro", "Destination Kafka Topic")
	kafkaUsername     = flag.String("kafka-username", "", "Destination Kafka Principal")
	kafkaPassword     = flag.String("kafka-password", "", "Destination Kafka Password")
	schemaRegistryUrl = flag.String("schema-registry-url", "http://localhost:8081", "Destination Schema Registry")

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

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	r1 := avrolib.NewGenericRecord(schema)
	r1.Set("seqNo", int64(1000))
	r1.Set("timestamp", int64(19834720000))

	r2 := avrolib.NewGenericRecord(schema)
	r2.Set("seqNo", int64(2000))
	r2.Set("timestamp", int64(19834723000))

	pipeline.
		Root(io.RoundRobin(10000000, []*avrolib.GenericRecord{r1, r2})).
		Apply(new(avro.GenericEncoder)).
		Buffer(5000).
		Throttle(4, time.Second).
		Apply(&avro.SchemaRegistryEncoder{
			Url: *schemaRegistryUrl,
			Subject: *kafkaTopic + "-value",
			CaCertFile: *kafkaCaCert,
		}).
		//Apply(new(std.Out)).TriggerEach(1).
		Apply(&kafka1.Sink{
			Topic: *kafkaTopic,
			ProducerConfig: kafka1.ConfigMap{
				"bootstrap.servers": *kafkaBootstrap,
				"security.protocol": "SASL_SSL",
				"sasl.mechanisms":   "PLAIN",
				"sasl.username":     *kafkaUsername,
				"sasl.password":     *kafkaPassword,
				"linger.ms":         50,
				"ssl.ca.location":   *kafkaCaCert,
				"compression.type":  "snappy",
				//"debug": 			"protocol,cgrp",
			}}).Apply(new(std.Out))
	pipeline.Run()

}
