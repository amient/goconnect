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
	"github.com/amient/goconnect/coder/str"
	"github.com/amient/goconnect/io"
	"github.com/amient/goconnect/io/std"
	"github.com/amient/goconnect/network"
	"strings"
)

var (
	//runner options
	peers = flag.String("peers", "127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003", "Coma-separated list of host:port peers")
)

func main() {

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry()).Par(4)

	pipeline.
		Root(io.From([]string{"aaa\tbbb\tccc", "ddd", "eee", "fff", "ggg\thhh"})).
		Apply(str.Split("\t")).Par(4). //same as FlatMap(func(in string, out chan string) { for _, s := range strings.Split(in, "\t") { out <- s } }).
		//coder: string -> []uint8
		Apply(new(network.NetRoundRobin)).
		//coder: []uint8 -> string
		Map(func(in string) string { return strings.ToUpper(in) }).Par(4).
		//coder: string -> []uint8
		Apply(new(network.NetMergeOrdered)).
		Apply(new(std.Out)).TriggerEach(1)//TODO .Limit(7) doesn't work on networked pipeliens yet

	network.Runner(pipeline, *peers)
	//pipeline.Run()
}
