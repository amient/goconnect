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
	numProduced uint64
	deliveries  chan kafka.Event
}

func (sink *Sink) InType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (sink *Sink) Process(input *goc.Element) {
	var err error

	if sink.producer == nil {
		sink.producer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": sink.Bootstrap,
		})
		sink.deliveries = make(chan kafka.Event)
		if err != nil {
			panic(err)
		}
		go func() {
			for e := range sink.deliveries {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						panic(fmt.Errorf("Delivery failed: %v\n", ev.TopicPartition))
					} else {
						input.Ack()
					}
				}
			}
		}()

	}
	kv := input.Value.(goc.KVBytes)

	defer sink.updateCounter()
	err = sink.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink.Topic},
		Key:            kv.Key,
		Value:          kv.Value,
		Timestamp:      *input.Timestamp,
	}, sink.deliveries)

	if err != nil {
		panic(err)
	}

}

func (sink *Sink) Flush() error {
	if sink.numProduced > 0 {
		log.Println("Kafka Sink Flush - Number of Produced Messages", sink.numProduced)
		numNotFlushed := sink.producer.Flush(15 * 1000)
		if numNotFlushed > 0 {
			return fmt.Errorf("could not flush all messages in timeout, numNotFlushed: %d", numNotFlushed)
		} else {
			sink.numProduced = 0
		}
	}
	return nil
}

func (sink *Sink) Close() error {
	if sink.producer != nil {
		defer log.Printf("Closed Kafka Producer")
		sink.producer.Close()
		close(sink.deliveries)
	}
	return nil
}

func (sink *Sink) updateCounter() {
	sink.numProduced++
}
