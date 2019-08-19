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
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func NewServer(addr string) *Server {
	recv := Server{
		Rand:      rand.Int63(),
		addr:      addr,
		Assigned:  make(chan bool, 1),
		quit:      make(chan bool, 1),
		receivers: make(map[uint16]*TCPReceiver),
	}
	recv.NewReceiver(0) //default TCPReceiver for node discovery
	return &recv
}

type Server struct {
	ID        uint16
	Addr      net.Addr
	Rand      int64
	Assigned  chan bool
	numNodes  uint32
	ln        *net.TCPListener
	addr      string
	quit      chan bool
	receivers map[uint16]*TCPReceiver
	lock      sync.Mutex
}

func (server *Server) Close() error {
	log.Printf("CLOSE SERVER[%v] num.receivers = %d", server.addr, len(server.receivers))
	server.quit <- true
	<-server.quit
	return nil
}

func (server *Server) Start() error {
	var err error
	ipv4addr, _ := net.ResolveTCPAddr("tcp4", server.addr)
	if server.ln, err = net.ListenTCP("tcp", ipv4addr); err != nil {
		return err
	} else {
		server.Addr = server.ln.Addr()
		server.ln.SetDeadline(time.Now().Add(1e9))
		go func() {
			for {
				select {
				case <-server.quit:
					server.ln.Close()
					server.quit <- true
					return
				default:
					server.ln.SetDeadline(time.Now().Add(1e9))
					if conn, err := server.ln.Accept(); err != nil {
						if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
							continue
						}
						panic(err)
					} else {
						duplex := NewDuplex(conn)
						handlerId := duplex.readUInt16()
						remoteNodeId := duplex.readUInt16()
						server.lock.Lock()
						receiver, exists := server.receivers[handlerId]
						server.lock.Unlock()
						if !exists {
							duplex.writeUInt16(0)
							duplex.Flush()
							duplex.Close()
						} else {
							//reply that the channel has been setup
							duplex.writeUInt16(handlerId)
							duplex.Flush()
							receiver.duplex[remoteNodeId] = duplex
							go receiver.handle(duplex, conn)
						}
					}
				}
			}
		}()
	}
	return nil
}
func (server *Server) NewReceiver(handlerId uint16) *TCPReceiver {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.receivers[handlerId] = &TCPReceiver{
		id:       handlerId,
		server:   server,
		eosCount: int32(server.numNodes),
		duplex:   make(map[uint16]*Duplex),
		down:     make(chan *goconnect.Element, 100), //TODO the capacity should be the number of nodes
	}
	if handlerId > 0 {
		//log.Printf("REGISTER[%v] %d", server.addr, handlerId)
	}
	return server.receivers[handlerId]

}

type TCPReceiver struct {
	id       uint16
	server   *Server
	down     chan *goconnect.Element
	eosCount int32
	refCount int32
	duplex   map[uint16]*Duplex
}

func (h *TCPReceiver) ID() uint16 {
	return h.id
}

func (h *TCPReceiver) Ack(upstreamNodeId uint16, uniq uint64) error {
	//log.Printf("NODE[%d] STAGE[%d] upstream  ACK(%d) >> NODE[%d]", h.server.ID, h.ID(), uniq, upstreamNodeId)
	duplex := h.duplex[upstreamNodeId]
	duplex.writeUInt16(1) //magic
	duplex.writeUInt64(uniq)
	return duplex.Flush()

}

func (h *TCPReceiver) handle(duplex *Duplex, conn net.Conn) {
	atomic.AddInt32(&h.refCount, 1)
	if h.id > 0 {
		//log.Printf("OPEN[%v/%d] refCount=%d", h.server.addr, h.ID, refCount)
		defer h.Close()
	}
	defer duplex.Close()
	defer conn.Close()
	for {
		//log.Printf("NEXT[%v/%d]", h.server.addr, h.NodeID)
		magic := duplex.readUInt16()
		switch magic {
		case 0: //eof
			log.Printf("NODE[%d] EOF HANDLER %d", h.server.ID, h.ID())
			return
		case 1: //node identification
			id := duplex.readUInt16() + 1
			rand := int64(duplex.readUInt64())
			h.server.numNodes = duplex.readUInt32()
			if rand == h.server.Rand {
				log.Printf("Assignment: Node ID %d is listening on %v", id, conn.LocalAddr().String())
				if h.server.ID != id {
					h.server.ID = id
					h.server.Assigned <- true
				}
			}
			return
		case 2: //downstream element
			//read stamp first
			fromNodeId := duplex.readUInt16()
			unix := int64(duplex.readUInt64())
			uniq := duplex.readUInt64()
			stamp := goconnect.Stamp{
				Unix: unix,
				Uniq: uniq,
			}
			//element value second
			value := duplex.readSlice()
			h.down <- &goconnect.Element{
				Stamp:      stamp,
				Value:      value,
				FromNodeId: fromNodeId,
			}
			//log.Printf("RECEIVED[%v/%d] %v element: %v", h.server.addr, h.NodeID, stamp, value)
		case 3: //eos
			/*fromNodeId := */ duplex.readUInt16()
			if atomic.AddInt32(&h.eosCount, -1) == 0 {
				//log.Printf("NODE[%d] STAGE[%d] NETWORK EOS ", h.server.ID, h.id)
				close(h.down)
			} else {
				//log.Printf("NODE[%d] STAGE[%d] NETWORK STILL ACTIVE %d", h.server.ID, h.id, h.eosCount)
			}
		default:
			panic(fmt.Errorf("unknown magic byte %d", magic))
		}
	}

}

func (h *TCPReceiver) Elements() <-chan *goconnect.Element {
	return h.down
}

func (h *TCPReceiver) Close() error {
	refCount := atomic.AddInt32(&h.refCount, -1)
	log.Printf("CLOSE[%v/%d] refCount=%d", h.server.addr, h.ID(), refCount)
	if refCount == 0 {
		delete(h.server.receivers, h.id)
	}
	return nil
}
