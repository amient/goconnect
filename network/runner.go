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

package network

import (
	"github.com/amient/goconnect"
	"log"
	"strings"
	"sync"
	"time"
)

func Runner(pipeline *goc.Pipeline, addrLists ...string) {

	addrs := make([]string, 0)
	for _, list := range addrLists {
		addrs = append(addrs, strings.Split(list, ",")...)
	}

	localNodes := JoinCluster(addrs...)

	graphs := make([]goc.Graph, len(localNodes))
	for i, node := range localNodes {
		graphs[i] = goc.ConnectStages(node, pipeline)
	}

	start := time.Now()
	log.Println("Running all graphs")
	goc.RunGraphs(graphs...)

	log.Printf("All stages completed in %f0.0 s", time.Now().Sub(start).Seconds())
	for _, node := range localNodes {
		node.server.Close()
	}

}

func JoinCluster(nodes ...string) []*Node {
	//start all nodes that can listen on this host
	instances := make([]*Node, 0)
	for _, node := range nodes {
		if instance, err := NewNode(node, nodes); err != nil {
			log.Println(err)
		} else {
			instances = append(instances, instance)
		}
	}

	if len(instances) == 0 {
		panic("no instances assigned")
	}

	//join the cluster
	log.Printf("Joining Cluser of %d nodes with %d running in this process", len(nodes), len(instances))
	join := sync.WaitGroup{}
	for _, instance := range instances {
		join.Add(1)
		go func(instance *Node) {
			instance.Join(nodes)
			join.Done()
		}(instance)
	}
	join.Wait()

	return instances
}
