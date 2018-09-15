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

package kafka1

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"reflect"
)

type Sink struct {
	Bootstrap   string
	Topic       string
	producer    *kafka.Producer
	//deliveries  chan kafka.Event
	numProduced uint64
	numAcked 	uint64

}

func (sink *Sink) InType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (sink *Sink) Process(input *goc.Element) {
	var err error

	if sink.producer == nil {
		sink.producer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": sink.Bootstrap,
			"go.delivery.reports": true,
		})
		//sink.deliveries = make(chan kafka.Event, 100)
		if err != nil {
			panic(err)
		}
		go func() {
			for e := range sink.producer.Events() {
				sink.processKafkaEvent(e)
			}
		}()

	}
	kv := input.Value.(goc.KVBytes)

	for {
		select {
			case sink.producer.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &sink.Topic, Partition: kafka.PartitionAny},
				Key:            kv.Key,
				Value:          kv.Value,
				Timestamp:      *input.Timestamp,
				Opaque:         input,
			}:
				sink.numProduced++
				return
			case e := <- sink.producer.Events():
				sink.processKafkaEvent(e)
		}
	}
}

func (sink *Sink) processKafkaEvent(e kafka.Event) {
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			panic(fmt.Errorf("Delivery failed: %v\n", ev.TopicPartition))
		} else {
			ev.Opaque.(*goc.Element).Ack()
			sink.numAcked++
			if sink.numAcked == sink.numProduced {
				log.Println("Kafka Producer in a clean state")
			}
		}
	}
}

func (sink *Sink) Close() error {
	if sink.producer != nil {
		defer log.Println("Closed Kafka Sink - Number of Produced Messages", sink.numProduced)
		sink.producer.Close()
		//close(sink.deliveries)
	}
	return nil
}

