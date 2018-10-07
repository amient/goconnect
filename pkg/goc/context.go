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
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var minBufSize = 32

type PC struct {
	Uniq           uint64
	UpstreamNodeId uint16
	Checkpoint     *Checkpoint
}

type P struct {
	u        uint64
	upstream uint64
	complete *int64
}

type ProcessContext struct {
	Emit func(value interface{})
}

type Receiver interface {
	Elements() <-chan *Element
	Ack(upstreamNodeId uint16, uniq uint64) error
}

type Sender interface {
	Acks() <-chan uint64
	Send(element *Element)
	Eos()
	Close() error
}

type Connector interface {
	GetNodeID() uint16
	MakeReceiver(stage uint16) Receiver
	GetNumPeers() uint16
	NewSender(nodeId uint16, stage uint16) Sender
	GenerateStageID() uint16
}

func NewContext(connector Connector, def *Def) *Context {
	context := Context{
		stage:     connector.GenerateStageID(),
		connector: connector,
		def:       def,
		output:    make(chan *Element, minBufSize),
		completed: make(chan bool),
	}
	return &context
}

type Context struct {
	Emit           func(element *Element)
	Ack            func(uniq uint64)
	Close          func()
	up             *Context
	stage          uint16
	output         chan *Element
	connector      Connector
	receiver       Receiver
	def            *Def
	autoi          uint64
	highestPending uint64
	highestAcked   uint64
	closed         bool
	completed      chan bool
}

func (c *Context) GetNodeID() uint16 {
	return c.connector.GetNodeID()
}

func (c *Context) GetStage() uint16 {
	return c.stage
}

func (c *Context) GetNumPeers() uint16 {
	return c.connector.GetNumPeers()
}

func (c *Context) GetReceiver() Receiver {
	if c.receiver == nil {
		c.receiver = c.connector.MakeReceiver(c.stage)
	}
	return c.receiver
}

func (c *Context) MakeSender(targetNodeId uint16) Sender {
	sender := c.connector.NewSender(targetNodeId, c.stage)
	go func() {
		for stamp := range sender.Acks() {
			c.up.Ack(stamp)
		}
	}()
	return sender
}

func (c *Context) MakeSenders() []Sender {
	senders := make([]Sender, c.GetNumPeers())
	for peer := uint16(1); peer <= c.GetNumPeers(); peer++ {
		senders[peer-1] = c.MakeSender(peer)
	}
	return senders
}

func (c *Context) Start() {

	switch fn := c.def.Fn.(type) {

	case FilterFn:
		c.Close = c.close
		c.Ack = c.up.Ack
		c.runVerticalGroup(func() {
			for e := range c.up.output {
				if fn.Pass(e.Value) {
					c.output <- e
				} else {
					e.Ack()
				}
			}
		})

	case MapFn:
		c.Close = c.close
		c.Ack = c.up.Ack
		c.runVerticalGroup(func() {
			for e := range c.up.output {
				c.output <- &Element{
					Value: fn.Process(e.Value),
					Stamp: e.Stamp,
					ack: func(uniq uint64) {
						c.up.Ack(uniq)
					},
				}
			}
		})

	case Root:
		//TODO make sure root behaviour with par > 1 is predictable
		pending := make(chan PC, minBufSize)
		c.initializeCheckpointer(1000000, pending) //TODO configurable capacity
		c.Emit = func(element *Element) {
			element.ack = c.Ack
			element.Stamp.Uniq = atomic.AddUint64(&c.autoi, 1)
			c.highestPending = element.Stamp.Uniq
			if element.Stamp.Unix == 0 {
				element.Stamp.Unix = time.Now().Unix()
			}
			pending <- PC{
				Uniq:           element.Stamp.Uniq,
				Checkpoint:     &element.Checkpoint,
				UpstreamNodeId: element.FromNodeId,
			}
			c.output <- element
		}
		c.runVerticalGroup(func() {
			fn.Run(c)
		})

	case Transform:
		//TODO disallow par > 1 for network transforms
		pending := make(chan PC, minBufSize)
		c.initializeNetworkBuffer(10000, pending) //TODO configurable capacity
		c.Emit = func(element *Element) {
			element.ack = c.Ack
			if element.Stamp.Uniq > c.highestPending {
				c.highestPending = element.Stamp.Uniq
			}
			pending <- PC{
				Uniq:           element.Stamp.Uniq,
				Checkpoint:     &element.Checkpoint,
				UpstreamNodeId: element.FromNodeId,
			}
			c.output <- element
		}
		c.runVerticalGroup(func() {
			fn.Run(c.up.output, c)
		})

	case Sink:
		c.Close = c.close
		c.Ack = c.up.Ack
		n := uint32(0)
		c.runVerticalGroup(func() {
			if c.def.triggerEvery == 0 {
				for e := range c.up.output {
					fn.Process(e)
					if int(atomic.AddUint32(&n, 1)) % c.def.triggerEach == 0 {
						fn.Flush()
					}
				}
				fn.Flush()
			} else {
				ticker := time.NewTicker(c.def.triggerEvery).C
				for {
					select {
					case e, ok := <-c.up.output:
						if !ok {
							fn.Flush()
							return
						} else {
							fn.Process(e)
						}
					case <-ticker:
						fn.Flush()
					}
				}
			}
		})

	case ElementWise:
		pending := make(chan P, minBufSize)
		c.initializeElementWiseBuffer(1000, pending) //TODO configurable capacity
		c.runVerticalGroup(func() {
			for e := range c.up.output {
				complete := int64(0)
				ctx := ProcessContext{
					Emit: func(value interface{}) {
						stamp := Stamp{Uniq: atomic.AddUint64(&c.autoi, 1)}
						atomic.AddInt64(&complete, 1)
						pending <- P{
							upstream: e.Stamp.Uniq,
							complete: &complete,
							u:        stamp.Uniq,
						}
						c.output <- &Element{
							Value: value,
							Stamp: stamp,
							ack:   c.Ack,
						}
					},
				}
				fn.Process(e, ctx)
			}
		})

	case FoldFn:
		c.Close = c.close
		c.Ack = c.up.Ack
		//TODO make sure fold behaves correctly under par >
		c.runVerticalGroup(func() {
			buffer := make([]uint64, 0, c.def.triggerEach)
			isComplete := true
			trigger := func() {
				swap := buffer
				buffer = make([]uint64, 0, c.def.triggerEach)
				e := fn.Collect()
				e.ack = func(uniq uint64) {
					for _, u := range swap {
						c.up.Ack(u)
					}
				}
				c.output <- &e
				isComplete = true
			}
			//TODO apply TriggerEvery as well
			for e := range c.up.output {
				fn.Process(e.Value)
				isComplete = false
				buffer = append(buffer, e.Stamp.Uniq)
				if len(buffer) == cap(buffer) {
					trigger()
				}
			}
			if !isComplete {
				trigger()
			}
		})

	default:
		panic(fmt.Errorf("unknown Fn Type: %v", reflect.TypeOf(fn)))
	}
}

func (c *Context) runVerticalGroup(f func()) {
	verticalGroupStart := sync.WaitGroup{}
	verticalGroupFinish := sync.WaitGroup{}
	for i := 1; i <= c.def.maxVerticalParallelism; i++ {
		verticalGroupStart.Add(1)
		verticalGroupFinish.Add(1)
		go func() {
			verticalGroupStart.Done()
			f()
			verticalGroupFinish.Done()
		}()
	}
	verticalGroupStart.Wait()
	go func() {
		verticalGroupFinish.Wait()
		close(c.output)
		c.Close()
	}()
}

func (c *Context) initializeCheckpointer(cap int, pending chan PC) {
	acks := make(chan uint64, cap)
	c.Ack = func(uniq uint64) {
		acks <- uniq
	}
	terminate := make(chan bool)
	terminating := false
	c.Close = func() {
		terminate <- true
	}
	pendingSuspendable := pending
	pendingCheckpoints := make(map[uint64]*PC, cap)
	acked := make(map[uint64]bool, cap)
	watermark := make(Watermark)
	pendingCommitReuqest := false

	var commits chan Watermark
	var commitRequests chan bool
	commitable, isCommitable := c.def.Fn.(Root)
	if isCommitable {
		commits = make(chan Watermark)
		commitRequests = make(chan bool)
		go func() {
			commitRequests <- true
			defer close(commits)
			defer close(commitRequests)
			for !c.closed {
				select {
				case checkpoint, ok := <-commits:
					if ok {
						if commitable != nil {
							commitable.Commit(checkpoint)
						}
						commitRequests <- true
					}
				}
			}
		}()
	}

	doCommit := func() {
		if len(watermark) > 0 {
			pendingCommitReuqest = false
			if isCommitable {
				//c.log("Commit Checkpoint: %v", watermark)
				commits <- watermark
			}
			watermark = make(map[int]interface{})
		}
	}

	maybeTerminate := func() {
		if terminating {
			if len(watermark) == 0 && c.highestPending == c.highestAcked {
				clean := (pendingCommitReuqest || !isCommitable) && len(pendingCheckpoints) == 0
				if clean {
					//c.log("Completed - closing")
					close(pending)
					close(acks)
					acks = nil
					c.close()
				}
			} else {
				//c.log("Awaiting Completion highestPending: %d, highestAcked: %d", c.highestPending, c.highestAcked)
			}
		}
	}

	resolveAcks := func(uniq uint64, action string) {
		if p, exists := pendingCheckpoints[uniq]; exists {
			if _, alsoExists := acked[uniq]; alsoExists {
				delete(pendingCheckpoints, uniq)
				delete(acked, uniq)
				watermark[p.Checkpoint.Part] = p.Checkpoint.Data
				if p.UpstreamNodeId > 0 {
					c.receiver.Ack(p.UpstreamNodeId, p.Uniq)
				} else if c.up != nil && !isCommitable {
					//commitables have to ack their upstream manually
					c.up.Ack(uniq)
				}

				//c.log(action+"(%v) RESOLVED PART %d DATA %v PENDING %d pendingCommitReuqest = %v\n", uniq, p.Checkpoint.Part, p.Checkpoint.Data, len(pendingCheckpoints), pendingCommitReuqest)
			}
		}

		if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
			//release the backpressure after capacity is freed
			pendingSuspendable = pending
		}

		//commit if pending commit request or not commitable in which case commit requests are never fired
		if pendingCommitReuqest || !isCommitable {
			doCommit()
		}

		maybeTerminate()
	}

	go func() {
		for !c.closed {
			select {
			case <-terminate:
				terminating = true
				maybeTerminate()
			case <-commitRequests:
				pendingCommitReuqest = true
				doCommit()
				maybeTerminate()
			case uniq := <-acks:
				if uniq > c.highestAcked {
					c.highestAcked = uniq
				}
				acked[uniq] = true
				resolveAcks(uniq, "ACK")

			case p := <-pendingSuspendable:
				pendingCheckpoints[p.Uniq] = &p

				resolveAcks(p.Uniq, "EMIT")

				if len(pendingCheckpoints) == cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
					pendingSuspendable = nil
					//c.log("STAGE[%d] Applying backpressure, pending acks: %d\n", c.stage, len(pendingCheckpoints))
				} else if len(pendingCheckpoints) > cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
				}

			}
		}
	}()
}

func (c *Context) initializeElementWiseBuffer(i int, pending chan P) {
	terminate := make(chan bool)
	terminating := false
	c.Close = func() {
		terminate <- true
	}
	acks := make(chan uint64, 1000) //TODO configurable ack capacity
	c.Ack = func(uniq uint64) {
		acks <- uniq
	}
	numAcked := uint64(0)
	go func() {
		groups := make(map[uint64]*P)
		maybeTerminate := func() {
			if terminating {
				if len(groups) == 0 && numAcked == c.autoi {
					c.log("Completed - closing")
					close(acks)
					c.close()
				}
			}
		}
		for !c.closed {
			select {
			case <-terminate:
				terminating = true
				maybeTerminate()
			case p2, ok := <-pending:
				if ok {
					groups[p2.u] = &p2
				} else {
					pending = nil
				}
			case uniq, ok := <-acks:
				if ok {
					atomic.AddUint64(&numAcked, 1)
					p := groups[uniq]
					delete(groups, uniq)
					if atomic.AddInt64(p.complete, -1) == 0 {
						c.up.Ack(p.upstream)
						maybeTerminate()
					}
				} else {
					acks = nil
					maybeTerminate()
				}
			}
		}
	}()

}

func (c *Context) initializeNetworkBuffer(cap int, pending chan PC) {
	acks := make(chan uint64, cap)
	c.Ack = func(uniq uint64) {
		acks <- uniq
	}
	terminate := make(chan bool)
	terminating := false
	c.Close = func() {
		terminate <- true
	}
	pendingSuspendable := pending
	pendingCheckpoints := make(map[uint64]*PC, cap)
	acked := make(map[uint64]bool, cap)

	maybeTerminate := func() {
		if terminating {
			if len(pendingCheckpoints) == 0 && c.highestPending == c.highestAcked {
				//c.log("Completed - closing")
				close(pending)
				close(acks)
				acks = nil
				c.close()
			} else {
				//c.log("Awaiting Completion highestPending: %d, highestAcked: %d", c.highestPending, c.highestAcked)
			}
		}
	}

	resolveAcks := func(uniq uint64, action string) {
		if p, exists := pendingCheckpoints[uniq]; exists {
			if _, alsoExists := acked[uniq]; alsoExists {
				delete(pendingCheckpoints, uniq)
				delete(acked, uniq)
				c.receiver.Ack(p.UpstreamNodeId, p.Uniq)
				//c.log(action+"(%v) RESOLVED PART %d DATA %v PENDING %d pendingCommitReuqest = %v\n", uniq, p.Checkpoint.Part, p.Checkpoint.Data, len(pendingCheckpoints), pendingCommitReuqest)
			}
		}

		if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
			//release the backpressure after capacity is freed
			pendingSuspendable = pending
		}

		maybeTerminate()
	}

	go func() {
		for !c.closed {
			select {
			case <-terminate:
				terminating = true
				maybeTerminate()
			case uniq := <-acks:
				if uniq > c.highestAcked {
					c.highestAcked = uniq
				}
				acked[uniq] = true
				resolveAcks(uniq, "ACK")

			case p := <-pendingSuspendable:
				pendingCheckpoints[p.Uniq] = &p

				resolveAcks(p.Uniq, "EMIT")

				if len(pendingCheckpoints) == cap {
					//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
					pendingSuspendable = nil
					//c.log("STAGE[%d] Applying backpressure, pending acks: %d\n", c.stage, len(pendingCheckpoints))
				} else if len(pendingCheckpoints) > cap {
					panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
				}

			}
		}
	}()
}


func (c *Context) close() {
	if ! c.closed {
		c.completed <- true
		c.closed = true
		if fn, ok := c.def.Fn.(io.Closer); ok {
			if err := fn.Close(); err != nil {
				panic(err)
			}
		}

	}
}

func (c *Context) log(f string, args ... interface{}) {
	args2 := make([]interface{}, len(args)+2)
	args2[0] = c.GetNodeID()
	args2[1] = c.stage
	for i := range args {
		args2[i+2] = args[i]
	}
	if c.stage > 0 {
		log.Printf("NODE[%d] STAGE[%d] "+f, args2...)
	}
}
