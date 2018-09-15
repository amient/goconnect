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
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder"
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"strings"
)

var data = []string{
	"<name>Adam</name>", "<name>Albert</name>", "<name>Alice</name>", "<name>Alex</name>",
	"<name>Bart</name>", "<name>Bob</name>", "<name>Brittney</name>", "<name>Brenda</name>",
	"<name>Cecilia</name>", "<name>Chad</name>", "<name>Elliot</name>", "<name>Wojtek</name>",
}

func main() {

	pipeline := goc.NewPipeline(coder.Registry())

	//root source of text elements
	// TODO generated lists are one of the examples which must be coordinated and run on any one instance
	messages := pipeline.Root(io.From(data))

	//extract names with custom Map fn (coders satisfying []byte => xml are injected by the pipeline)
	extracted := messages.Map(func(input gocxml.Node) string {
		return input.Children()[0].Children()[0].Text()
	})

	//remove all names containing letter 'B' with custom Filter fn
	filtered := extracted.Filter(func(input string) bool {
		return !strings.Contains(input, "B")
	})

	filtered.Apply(std.StdOutSink())
	//filtered.
	//	Apply(gocstring.Encoder()).
	//	Apply(kafka1.NilKeyEncoder()).
	//	Apply(&kafka1.Sink{
	//		Bootstrap: "localhost:9092",
	//		Topic:     "test",
	//	})

	////TODO total aggregation using custom fn
	//total := filtered.Transform(func(input chan string, output chan int) {
	//	l := 0
	//	for b := range input {
	//		l += len(b)
	//	}
	//	output <- l
	//})
	//
	////output the aggregation result by applying a general StdOutSink transform
	////TODO StdOOut sink must be network-merged to the single instance which last joined the group
	//total.Apply(std.StdOutSink())

	pipeline.Run()

}

//TODO next step is adding networking-friendly checkpointer with pipeline options of optimistic or pessimistic one
