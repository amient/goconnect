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
	"fmt"
	"github.com/amient/goconnect/examples/kafka-custom-avro-stdout/io.amient.kafka.metrics"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/coder/avro"
	"github.com/amient/goconnect/io/kafka1"
	"github.com/amient/goconnect/io/std"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"reflect"
)

var (
	kafkaSourceBootstrap = flag.String("kafka.source.bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaSourceGroup     = flag.String("kafka.source.group", "goc-avro-poc", "Source Kafka Consumer Group")
	kafkaSourceTopic     = flag.String("kafka.source.topic", "_metrics", "Source Kafka Topic")
)

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	consumerConfig := kafka.ConfigMap{"bootstrap.servers": *kafkaSourceBootstrap, "group.id": *kafkaSourceGroup}

	pipeline.Root(&kafka1.Source{*kafkaSourceTopic, consumerConfig}).
		Apply(new(KafkaMetricsAvroRegistry)).
		Apply(new(avro.GenericDecoder)).
		Apply(new(std.Out)).TriggerEach(1)

	pipeline.Run()

}

type KafkaMetricsAvroRegistry struct{}

func (m *KafkaMetricsAvroRegistry) InType() reflect.Type {
	return goconnect.KVBinaryType
}

func (m *KafkaMetricsAvroRegistry) OutType() reflect.Type {
	return avro.BinaryType
}

func (m *KafkaMetricsAvroRegistry)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		kvBinary := input.(*goconnect.KVBinary)
		switch kvBinary.Value[0] {
		case 0:
			panic("cannot use rest schema registry for kafka metrics formats")
		case 1:
			switch int(kvBinary.Value[1]) {
			case 1:
				return &avro.Binary{
					Schema: io_amient_kafka_metrics.MeasurementSchemaV1,
					Data:   kvBinary.Value[2:],
				}
			default:
				panic("there is no other version at this time")
			}
		case '{':
			fallthrough
		default:
			panic(fmt.Errorf("invalid kafka metrics avro format"))
		}
	}
}
