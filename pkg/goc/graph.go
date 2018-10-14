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

package goc

import (
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
)

type Graph []*Context

func ConnectStages(connector Connector, pipeline *Pipeline) Graph {
	graph := make(Graph, len(pipeline.Defs))
	log.Printf("Applying pipline of %d stages", len(graph))
	for stageId, def := range pipeline.Defs {
		graph[def.Id] = NewContext(connector, uint16(stageId + 1), def)
		if def.Id > 0 {
			graph[def.Id].up = graph[def.Up.Id]
		}
	}
	return graph
}

func RunGraphs(graphs ...Graph) {
	//this method assumes a single source in each graph
	sources := make([]*Context, len(graphs))
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sigterm)},
	}
	runningStages := 0
	for i, graph := range graphs {
		sources[i] = graph[0]
		for _, ctx := range graph {
			log.Printf("Context[%d] Stage %d %v\n", runningStages, ctx.stage, reflect.TypeOf(ctx.def.Fn))
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.completed)})
			ctx.Start()
			runningStages++
		}
	}

	for {
		if chosen, value, _ := reflect.Select(cases); chosen == 0 {
			//FIXME busy pipelines sometimes refuse to terminate upon signalling
			log.Printf("Caught signal %v: Cancelling\n", value.Interface())
			for _, source := range sources {
				source.Close()
			}
		} else {
			runningStages--
			//log.Printf("Graph Finished Stage[%v], num running stages: %v", chosen, runningStages)
			if runningStages == 0 {
				return
			}
		}

	}
}
