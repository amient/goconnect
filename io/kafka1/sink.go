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
	"github.com/amient/goconnect"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"reflect"
	"time"
)

type Sink struct {
	Topic           string
	ProducerConfig  ConfigMap
	//numProduced     int32 //FIXME this must be done in the Materialize()..
}

func (sink *Sink) InType() reflect.Type {
	return goconnect.KVBinaryType
}

func (sink *Sink) Process(input *goconnect.Element, ctx *goconnect.Context) {
	var err error

	var producer *kafka.Producer
	config := kafka.ConfigMap{}
	for k,v := range sink.ProducerConfig {
		config.SetKey(k, v)
	}
	if ctx.Get(0) != nil {
		producer = ctx.Get(0).(*kafka.Producer)
	} else {
		sink.ProducerConfig["go.delivery.reports"] = true
		producer, err = kafka.NewProducer(&config)
		if err != nil {
			panic(err)
		}
		ctx.Put(0, producer)
		go func() {
			for e := range producer.Events() {
				sink.processKafkaEvent(e)
			}
		}()

	}
	kv := input.Value.(*goconnect.KVBinary)

	for {
		select {
		case producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &sink.Topic, Partition: kafka.PartitionAny},
			Key:            kv.Key,
			Value:          kv.Value,
			Timestamp:      time.Unix(input.Stamp.Unix, 0),
			Opaque:         input,
		}:
			//TODO atomic context update of numProduced
			//atomic.AddInt32(&sink.numProduced, 1)
			return
		case e := <-producer.Events():
			sink.processKafkaEvent(e)
		}
	}
}

func (sink *Sink) Flush(ctx *goconnect.Context) error {
	if ctx.Get(0) != nil {
		producer := ctx.Get(0).(*kafka.Producer)
		var outstanding int
		for i := 1; i < 10; i ++ {
			outstanding = producer.Flush(30000)
			if outstanding == 0 {
				return nil
			}
		}
		return fmt.Errorf("failed to flush all produced messages, outstading: %v", outstanding)
	}
	return nil
}

func (sink *Sink) processKafkaEvent(e kafka.Event) {
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			panic(fmt.Errorf("Delivery failed: %v\n", ev.TopicPartition))
		} else {
			ev.Opaque.(*goconnect.Element).Ack()
			//n := atomic.AddInt32(&sink.numProduced, -1)
			//if n == 0 {
			//	log.Println("Kafka Sink in a clean state")
			//}
		}
	}
}

func (sink *Sink) Close(ctx *goconnect.Context) error {
	if ctx.Get(0) != nil {
		producer := ctx.Get(0).(*kafka.Producer)
		producer.Close()
	}
	return nil
}
