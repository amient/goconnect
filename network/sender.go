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
	"fmt"
	"github.com/amient/goconnect"
	"log"
	"net"
	"time"
)

func newSender(addr string, handlerId uint16, nodeId uint16) *TCPSender {
	return &TCPSender{
		addr:      addr,
		handlerId: handlerId,
		nodeId:    nodeId,
		acks:      make(chan uint64, 1),
	}
}

type TCPSender struct {
	addr      string
	handlerId uint16
	nodeId    uint16
	conn      net.Conn
	duplex    *Duplex
	acks      chan uint64
}

func (sender *TCPSender) Acks() <-chan uint64 {
	return sender.acks
}

func (sender *TCPSender) Close() error {
	//log.Printf("NODE[%d] TCPSender.close(%d)", sender.nodeId, sender.handlerId)
	close(sender.acks)
	return sender.duplex.Close()
}

func (sender *TCPSender) Start() error {
	//println("Open", sender.addr, sender.handlerId)
	for remainingAttempts := 100; remainingAttempts >= 0; remainingAttempts -- {
		var err error
		if sender.conn, err = net.Dial("tcp", sender.addr); err != nil {
			return err
		}
		sender.duplex = NewDuplex(sender.conn)
		sender.duplex.writeUInt16(sender.handlerId)
		sender.duplex.writeUInt16(sender.nodeId)
		sender.duplex.Flush()
		//this needs to block until the handler is open
		if sender.duplex.readUInt16() == sender.handlerId {
			break
		} else if remainingAttempts == 0 {
			panic(fmt.Errorf("ERROR[%v] TCPReceiver not registered %d", sender.addr, sender.handlerId))
		} else {
			sender.conn.Close()
			log.Printf("Retrying to connect to %v", sender.addr)
			time.Sleep(100 * time.Millisecond)
		}
	}

	go func() {
		d := sender.duplex
		for {
			magic := d.readUInt16()
			switch magic {
			case 0: //eos
				return
			case 1: //incoming ack from downstream
				sender.acks <- d.readUInt64()
			default:
				panic(fmt.Errorf("unknown magic byte %d", magic))
			}
		}
	}()
	return nil
}

func (sender *TCPSender) SendJoin(nodeId int, server *Server, numNodes int) {
	sender.duplex.writeUInt16(1) //magic
	sender.duplex.writeUInt16(uint16(nodeId))
	sender.duplex.writeUInt64(uint64(server.Rand))
	sender.duplex.writeUInt32(uint32(numNodes))
	sender.duplex.Flush()
	sender.duplex.readUInt16()
	sender.Close()
}

func (sender *TCPSender) Send(e *goconnect.Element) {
	sender.duplex.writeUInt16(2) //magic
	sender.duplex.writeUInt16(sender.nodeId)
	sender.duplex.writeUInt64(uint64(e.Stamp.Unix))
	sender.duplex.writeUInt64(e.Stamp.Uniq)
	sender.duplex.writeSlice(e.Value.([]byte))
	sender.duplex.Flush()
}

func (sender *TCPSender) Eos() {
	sender.duplex.writeUInt16(3)             //eos header
	sender.duplex.writeUInt16(sender.nodeId) //magic
	sender.duplex.Flush()
}
