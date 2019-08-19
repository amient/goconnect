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
	"github.com/amient/goconnect/io"
	"github.com/amient/goconnect/io/kafka1"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	kafkaBootstrap = flag.String("kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaTopic     = flag.String("kafka-topic", "test", "Destination Kafka Topic")
	kafkaUsername  = flag.String("username", "", "Kafka Principal")
	kafkaPassword  = flag.String("password", "", "Kafka Principal Password")

	data = []string{
		`<?xml version="1.0" encoding="UTF-8"?>
<transferTemplate version="4.00" id="baf9df73-45c2-4bb0-a085-292232ab66bc">
    <name>BASIC_TEMPLATE</name>
    <sourceAgentName>AGENT_JUPITER</sourceAgentName>
    <sourceAgentQMgr>QM_JUPITER</sourceAgentQMgr>
    <destinationAgentName>AGENT_SATURN</destinationAgentName>
    <destinationAgentQMgr>QM_JUPITER</destinationAgentQMgr>
    <fileSpecs>
        <item mode="binary" checksumMethod="MD5">
            <source recursive="false" disposition="leave">
                <file>/etc/passwd</file>
            </source>
            <destination type="directory" exist="overwrite">
                <file>/tmp</file>
            </destination>
        </item>
    </fileSpecs>
    <priority>0</priority>
</transferTemplate>`,
	}
)

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	pipeline.
		Root(io.RoundRobin(10000000, data)).Buffer(5000).
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
