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
	"time"
)

type ConsumerCheckpoint struct {
	Partition int32
	Offset    uint64
}

func (c ConsumerCheckpoint) String() string {
	return fmt.Sprintf("%d:%d", c.Partition, c.Offset)
}

type Source struct {
	Bootstrap string
	Topic     string
	Group     string
	consumer  *kafka.Consumer
	start     time.Time
	counter   map[int32]uint64
	total     uint64
}

func (source *Source) OutType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (source *Source) Do(context *goc.Context) {
	var err error
	source.counter = make(map[int32]uint64)
	source.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": source.Bootstrap,
		"group.id":          source.Group,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset":  "earliest", //TODO pass this as config
			"enable.auto.commit": "false",
		},
		"enable.auto.commit":       "false",
		"go.events.channel.enable": true,
	})
	if err != nil {
		panic(err)
	}

	log.Printf("Subscribing to kafka topic %s", source.Topic)
	if err := source.consumer.Subscribe(source.Topic, nil); err != nil {
		panic(err)
	}

	for event := range source.consumer.Events() {

		switch e := event.(type) {
		case kafka.AssignedPartitions: //not used
		case kafka.RevokedPartitions: //not used
		case *kafka.Message:
			if len(source.counter) == 0 {
				source.start = time.Now()
			}
			if _, contains := source.counter[e.TopicPartition.Partition]; !contains {
				source.counter[e.TopicPartition.Partition] = 0
			}
			source.counter[e.TopicPartition.Partition]++
			context.Emit(&goc.Element{
				Stamp: goc.Stamp{Unix: e.Timestamp.Unix()},
				Checkpoint: goc.Checkpoint{
					Part: int(e.TopicPartition.Partition),
					Data: e.TopicPartition.Offset,
				},
				Value: goc.KVBytes{
					Key:   e.Key,
					Value: e.Value,
				},
			})
		case kafka.PartitionEOF:
			source.total += source.counter[e.Partition]
			delete(source.counter, e.Partition)
			if len(source.counter) == 0 && source.total > 0 {
				log.Printf("EOF: Consumed %d in %f ms\n", source.total, time.Now().Sub(source.start).Seconds())
				source.total = 0
			}

		case kafka.Error:
			panic(e)
		}
	}
}

func (source *Source) Commit(checkpoint goc.Watermark) error {
	var offsets []kafka.TopicPartition
	for k, v := range checkpoint {
		offsets = append(offsets, kafka.TopicPartition{
			Topic:     &source.Topic,
			Partition: int32(k),
			Offset:    v.(kafka.Offset) + 1,
		})
	}
	if len(offsets) > 0 {
		if _, err := source.consumer.CommitOffsets(offsets); err != nil {
			return err
		} else {
			log.Printf("Kafka Commit Successful: %v", offsets)
		}
	}
	return nil
}

func (source *Source) Close() error {
	defer log.Println("Closed Kafka Consumer")
	return source.consumer.Close()
}
