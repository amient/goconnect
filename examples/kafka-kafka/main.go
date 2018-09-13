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
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
	"time"
)

var (
	//general pipleline arguments
	commitInterval = flag.Duration("commit-interval", 5*time.Second, "Commit interval of the whole connector")
	//
	kafkaSourceBootstrap = flag.String("kafka.source.bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaSourceGroup     = flag.String("kafka.source.group", "goconnect-mirror", "Source Kafka Consumer Group")
	kafkaSourceTopic     = flag.String("kafka.source.topic", "test", "Source Kafka Topic")
	kafkaSinkBootstrap   = flag.String("kafka.sink.bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaSinkTopic       = flag.String("kafka.sink.topic", "test-copy", "Destination Kafka Topic")
)

func main() {

	pipeline := goc.NewPipeline()

	source := pipeline.Root(&kafka1.Source{
		Bootstrap: *kafkaSourceBootstrap,
		Topic:     *kafkaSourceTopic,
		Group:     *kafkaSourceGroup,
	})

	source.Apply(&kafka1.Sink{
		Bootstrap: *kafkaSinkBootstrap,
		Topic:     *kafkaSinkTopic,
	})

	pipeline.Run()

}
