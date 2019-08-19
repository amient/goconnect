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
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/coder/xml"
	"github.com/amient/goconnect/io"
	"github.com/amient/goconnect/io/std"
	"strings"
)

var data = []string{
	"<name>Adam</name>", "<name>Albert</name>", "<name>Alice</name>", "<name>Alex</name>",
	"<name>Bart</name>", "<name>Bob</name>", "<name>Brittney</name>", "<name>Brenda</name>",
	"<name>Cecilia</name>", "<name>Chad</name>", "<name>Elliot</name>", "<name>Wojtek</name>",
}

func main() {

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry()).Par(2)

	//root source of text elements
	pipeline.
		Root(io.RoundRobin(500000, data)).Buffer(100000).
		Map(func(in xml.Node) string { return in.Children()[0].Children()[0].Text() }).
		Filter(func(input string) bool { return !strings.Contains(input, "B") }).Par(4).
		//
		Fold(0, func(acc int, in string) int { return acc + len(in) }).TriggerEach(50000).
		Filter(func(x int) bool { return x > 210000 }).
		//Count().TriggerEvery(500 * time.Millisecond).
		Apply(new(std.Out)).TriggerEach(1)

	//Apply(&kafka1.Sink{
	//	Topic:          "test",
	//	ProducerConfig: kafka.ConfigMap{
	//		"bootstrap.servers": "localhost:9092",
	//		"linger.ms": 100,
	//	},
	//})

	pipeline.Run()

}
