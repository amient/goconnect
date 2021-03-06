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
	"github.com/amient/goconnect/io/amqp09"
)

var (
	//sink arguments
	uri          = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("amqp-exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("amqp-exchange-type", "direct", "Exchange type - direct|fanout|kafkaSinkTopic|x-custom")
	queue        = flag.String("amqp-queue", "test", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("amqp-key", "", "AMQP binding key")

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

	amqp09.DeclareExchange(*uri, *exchange, *exchangeType, *queue)

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	pipeline.
		Root(io.RoundRobin(100000, data)).
		Apply(&amqp09.Sink{Uri: *uri, Exchange: *exchange, BindingKey: *bindingKey,})

	pipeline.Run()

}
