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
	"time"
)

func NewNode(addr string, nodes []string) (*Node, error) {
	server := NewServer(addr)
	if err := server.Start(); err != nil {
		return nil, err
	} else {
		return &Node{
			server: server,
			nodes:  nodes,
		}, nil
	}
}

type Node struct {
	server *Server
	nodes  []string
}

func (node *Node) GetNodeID() uint16 {
	return node.server.ID
}

func (node *Node) GetNumPeers() uint16 {
	return uint16(len(node.nodes))
}

func (node *Node) MakeReceiver(stageId uint16) goc.Receiver {
	return node.server.NewReceiver(stageId)
}

func (node *Node) NewSender(targetNodeId uint16, stageId uint16) goc.Sender {
	addr := node.nodes[targetNodeId-1]
	sender := newSender(addr, stageId, node.server.ID)
	if err := sender.Start(); err != nil {
		panic(err)
	}
	return sender
}

func (node *Node) Join(nodes []string) {
	//TODO pipelines which don't contain any network stages, i.e. forks, splits, merges shuffles
	//..don't have to join the cluster, members can join and leave whenever started/stopped
	//TODO the best way would be to make joining and leaving nodes completely dynamic
	//by introducing processing epochs - an epoch starts and ends whenever any node joins
	//or leaves the group. the previous epoch must complete and fully drain all remaining data
	//before the new epoch is started.
	// - unconstrained stages are simply drained and re-started
	// - constrained stages should use checkpoint storage to be able to resume from the previous
	//   epoch's last position in case the nomination process results in a different node(s) being selected.
	for nodeId := 0; nodeId < len(nodes); {
		addr := nodes[nodeId]
		s := newSender(addr, 0, uint16(nodeId))
		if err := s.Start(); err != nil {
			time.Sleep(time.Second)
			log.Printf("Waiting for node at %v fn join the cluster..", addr)
		} else {
			s.SendJoin(nodeId, node.server, len(nodes))
			nodeId ++
		}
	}
	<-node.server.Assigned
}
