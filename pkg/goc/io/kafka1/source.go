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

type ConsumerCheckpoint struct {
	Partition int32
	Offset    uint64
}

func (c ConsumerCheckpoint) String() string {
	return fmt.Sprintf("%d:%d", c.Partition, c.Offset)
}

type Source struct {
	Topic          string
	ConsumerConfig kafka.ConfigMap
}

func (source *Source) OutType() reflect.Type {
	return goc.KVBinaryType
}

func (source *Source) Run(context *goc.Context) {
	//var start time.Time
	var total uint64
	counter := make(map[int32]uint64)
	config := &source.ConsumerConfig
	config.SetKey("go.events.channel.enable", true)
	config.SetKey("enable.auto.commit", false)
	defaultTopicConfig, _ := config.Get("default.topic.config", kafka.ConfigMap{})
	SetConfig := func(key string, defVal kafka.ConfigValue) {
		v, _ := config.Get(key, defVal)
		defaultTopicConfig.(kafka.ConfigMap).SetKey(key, v)
	}
	SetConfig("enable.auto.commit", false)
	SetConfig("auto.offset.reset", "earliest")
	config.SetKey("default.topic.config", defaultTopicConfig)

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	context.Put(0, consumer)

	log.Printf("Subscribing to kafka topic %s with grup %q", source.Topic, (*config)["group.id"])
	if err := consumer.Subscribe(source.Topic, nil); err != nil {
		panic(err)
	}

	log.Printf("Consuming kafka topic %q", source.Topic)

	for {
		select {
		case <- context.Termination():
			return
		case event, ok := <-consumer.Events():
			if !ok {
				return
			}
			switch e := event.(type) {
			case kafka.AssignedPartitions: //not used
			case kafka.RevokedPartitions: //not used
			case *kafka.Message:
				//if len(counter) == 0 {
				//	start = time.Now()
				//}
				if _, contains := counter[e.TopicPartition.Partition]; !contains {
					counter[e.TopicPartition.Partition] = 0
				}
				counter[e.TopicPartition.Partition]++
				context.Emit(&goc.Element{
					Stamp: goc.Stamp{Unix: e.Timestamp.Unix()},
					Checkpoint: goc.Checkpoint{
						Part: int(e.TopicPartition.Partition),
						Data: e.TopicPartition.Offset,
					},
					Value: &goc.KVBinary{
						Key:   e.Key,
						Value: e.Value,
					},
				})
			case kafka.PartitionEOF:
				total += counter[e.Partition]
				delete(counter, e.Partition)
				if len(counter) == 0 && total > 0 {
					//log.Printf("EOF Consumed %d in %f s\n", total, time.Now().Sub(start).Seconds())
					total = 0
				}

			case kafka.Error:
				panic(e)
			}
		}
	}
}

func (source *Source) Commit(checkpoint goc.Watermark, ctx *goc.Context) error {
	consumer := ctx.Get(0).(*kafka.Consumer)
	var offsets []kafka.TopicPartition
	for k, v := range checkpoint {
		offsets = append(offsets, kafka.TopicPartition{
			Topic:     &source.Topic,
			Partition: int32(k),
			Offset:    v.(kafka.Offset) + 1,
		})
	}
	if len(offsets) > 0 {
		if _, err := consumer.CommitOffsets(offsets); err != nil {
			return err
		} else {
			//log.Printf("Kafka Commit Successful: %v", offsets)
		}
	}
	return nil
}

func (source *Source) Close(ctx *goc.Context) error {
	consumer := ctx.Get(0).(*kafka.Consumer)
	log.Println("Closing Kafka Consumer")
	//FIXME closing kafka consumer doesn't remove the client from the group, only after zk timeout
	return consumer.Close()
}
