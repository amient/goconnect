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
	"github.com/amient/goconnect/io/file"
	"github.com/amient/goconnect/io/std"
	"github.com/amient/goconnect/network"
	"strings"
)

var (
	//runner options
	peers = flag.String("peers", "127.0.0.1:19001,127.0.0.1:19002,127.0.0.1:19003", "Coma-separated list of host:port peers")
)

func main() {

	//TODO var subflow goc.MapFn =  str.Split('\n') >
	//TODO the flat map stage has to be able to resume mid-way through a file

	pipeline := goc.NewPipeline().WithCoders(coder.Registry()).Par(2)

	//root source of text elements
	pipeline.
		Root(&file.Source{"/tmp/test"}).
		Apply(new(network.NetRoundRobin)).
		//TODO Map( if gz then Apply(gzip.Decode), if bz then Apply(bzip.Decode) )
		Apply(new(file.Reader)).
		//TODO Map( subflow ).
		Apply(new(file.Text)).
		Map(func(in string) string { return strings.ToUpper(in) }).Par(4).
		//TODO Apply(new(network.NetMergeOrdered)). - this is not working atm because the there is a flat map after network spli
		Apply(new(std.Out)).TriggerEach(1)

	//pipeline.Run()
	network.Runner(pipeline, *peers)

}
