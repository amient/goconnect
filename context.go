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

package goconnect

import (
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"runtime/pprof"
	"sync/atomic"
	"time"
)

type PC struct {
	Uniq           uint64
	UpstreamNodeId uint16
	Checkpoint     *Checkpoint
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
}

func NewContext(connector Connector, stageId uint16, def *Def) *Context {
	context := Context{
		stage:        stageId,
		connector:    connector,
		def:          def,
		limitEnabled: def.limit > 0,
		termination:  make(chan bool, 2),                                //RootFn implementations must consume context.Termination() channel
		output:       make(chan *Element, def.maxVerticalParallelism*4), //TODO ouptut buffer may need to be bigger than factor of for, e.g. large FlatMaps
		completed:    make(chan bool),
		data:         make(map[int]interface{}),
	}
	return &context
}

type Context struct {
	Emit         func(element *Element)
	termination  chan bool
	ack          func(uniq uint64)
	stop         func(uniq uint64)
	Close        func()
	data         map[int]interface{}
	up           *Context
	stage        uint16
	output       chan *Element
	connector    Connector
	receiver     Receiver
	def          *Def
	limitEnabled bool
	limitCounter uint64
	autoi        uint64
	closed       bool
	completed    chan bool
}

func (c *Context) GetNodeID() uint16 {
	if c.connector == nil {
		return 1
	} else {
		return c.connector.GetNodeID()
	}
}

func (c *Context) GetStage() uint16 {
	return c.stage
}

func (c *Context) GetNumPeers() uint16 {
	c.checkConnector()
	return c.connector.GetNumPeers()
}

func (c *Context) GetReceiver() Receiver {
	c.checkConnector()
	if c.receiver == nil {
		c.receiver = c.connector.MakeReceiver(c.stage)
	}
	return c.receiver
}

func (c *Context) MakeSender(targetNodeId uint16) Sender {
	c.checkConnector()
	sender := c.connector.NewSender(targetNodeId, c.stage)
	go func() {
		for stamp := range sender.Acks() {
			c.up.ack(stamp)
		}
	}()
	return sender
}

func (c *Context) MakeSenders() []Sender {
	c.checkConnector()
	senders := make([]Sender, c.GetNumPeers())
	for peer := uint16(1); peer <= c.GetNumPeers(); peer++ {
		senders[peer-1] = c.MakeSender(peer)
	}
	return senders
}

func (c *Context) checkConnector() {
	if c.connector == nil {
		panic("the pipeline requires a network runner - use network.Runner(pipeline,...) instead of Pipeline.Run()")
	}
}

//FIXME instead of Put and Get on Context migrate all transforms that needs to materialized forms
func (c *Context) Put(index int, data interface{}) {
	c.data[index] = data
}

func (c *Context) Get(index int) interface{} {
	return c.data[index]
}

func (c *Context) Termination() <-chan bool {
	return c.termination
}


func (c *Context) Start() {

	switch fn := c.def.Fn.(type) {

	case Root:
		pending := make(chan PC, c.def.bufferCap)
		c.initializeCheckpointer(c.def.bufferCap, pending, fn)
		go func() {
			fn.Run(c)
			close(c.output)
			c.Close()
		}()

	case Processor:
		NewWorkerGroup(c, fn).Start(c.up.output)

	case Mapper:
		c.ack = c.up.ack
		c.stop = c.up.stop
		NewWorkerGroup(c,  &MapProcessor{fn}).Start(c.up.output)

	case Filter:
		c.ack = c.up.ack
		c.stop = c.up.stop
		NewWorkerGroup(c, &FilterProcessor{fn}).Start(c.up.output)


	case NetTransform:
		pending := make(chan PC, c.def.bufferCap)
		//TODO implement limit/stop for network buffer
		acks := make(chan uint64, c.def.bufferCap)
		c.ack = func(uniq uint64) {
			acks <- uniq
		}
		terminate := make(chan bool)
		terminating := false
		c.Close = func() {
			terminate <- true
		}
		pendingSuspendable := pending
		pendingCheckpoints := make(map[uint64]*PC, c.def.bufferCap)
		acked := make(map[uint64]bool, c.def.bufferCap)

		var highestPending uint64 //FIXME these two vars are volatile
		var highestAcked uint64
		maybeTerminate := func() {
			if terminating {
				if len(pendingCheckpoints) == 0 && highestPending == highestAcked {
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

		resolveAcks := func(uniq uint64) {
			if p, exists := pendingCheckpoints[uniq]; exists {
				if _, alsoExists := acked[uniq]; alsoExists {
					delete(pendingCheckpoints, uniq)
					delete(acked, uniq)
					c.receiver.Ack(p.UpstreamNodeId, p.Uniq)
					//c.log(action+"(%v) RESOLVED PART %d DATA %v PENDING %d pendingCommitReuqest = %v\n", uniq, p.Checkpoint.Part, p.Checkpoint.Data, len(pendingCheckpoints), pendingCommitReuqest)
				}
			}

			if len(pendingCheckpoints) < c.def.bufferCap && pendingSuspendable == nil {
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
					if uniq > highestAcked {
						highestAcked = uniq
					}
					acked[uniq] = true
					resolveAcks(uniq)

				case p := <-pendingSuspendable:
					pendingCheckpoints[p.Uniq] = &p

					resolveAcks(p.Uniq)

					if len(pendingCheckpoints) == c.def.bufferCap {
						//in order to apply backpressure this channel needs to be nilld but right now it hangs after second pendingSuspendable
						pendingSuspendable = nil
						//c.log("STAGE[%d] Applying backpressure, pending acks: %d\n", c.stage, len(pendingCheckpoints))
					} else if len(pendingCheckpoints) > c.def.bufferCap {
						panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", c.def.bufferCap))
					}

				}
			}
		}()

		c.Emit = func(element *Element) {
			//TODO network stage need implementing .stop for limit conditions to propagate through them
			if element.Stamp.Uniq > highestPending {
				highestPending = element.Stamp.Uniq
			}
			pending <- PC{
				Uniq:           element.Stamp.Uniq,
				Checkpoint:     &element.Checkpoint,
				UpstreamNodeId: element.FromNodeId,
			}
			c.output <- element
		}
		go func() {
			fn.Run(c.up.output, c)
			close(c.output)
			c.Close()
		}()

	case Sink:
		c.Close = c.close
		c.stop = c.up.stop
		go func() {
			unflushed := false
			n := 0
			trigger := func() {
				if unflushed {
					if err := fn.Flush(c); err != nil {
						panic(err)
					}
					unflushed = false
				}
			}
			var ticker <-chan time.Time
			if c.def.triggerEvery > 0 {
				ticker = time.NewTicker(c.def.triggerEvery).C
			}
			for {
				select {
				case <-ticker:
					trigger()
				case e, ok := <-c.up.output:
					if !ok {
						trigger()
						close(c.output)
						c.Close()
						return
					} else {
						e.ack = c.up.ack
						if !c.limitReached(e.Stamp.Uniq) {

							fn.Process(e, c)
							unflushed = true
							if c.def.triggerEach > 0 {
								n++
								if n%c.def.triggerEach == 0 {
									if err := fn.Flush(c); err != nil {
										panic(err)
									}
								}
							}
						}
					}
				}
			}

		}()

	case FoldFn:
		c.Close = c.close

		stops := make(chan uint64, 1)
		c.stop = func(uniq uint64) {
			stops <- uniq
		}
		acks := make(chan uint64, c.def.bufferCap)
		c.ack = func(uniq uint64) {
			acks <- uniq
		}

		go func() {
			pending := make(map[uint64][]uint64, c.def.bufferCap)
			buffer := make([]uint64, 0, c.def.triggerEach)
			isClean := true
			highestAcked := uint64(0)
			trigger := func() {
				if !isClean {
					e := fn.Collect()
					e.Stamp.Uniq = atomic.AddUint64(&c.autoi, 1)
					if !c.limitReached(e.Stamp.Uniq) {
						if e.Stamp.Unix == 0 {
							e.Stamp.Unix = time.Now().Unix()
						}
						swap := buffer
						buffer = make([]uint64, 0, c.def.triggerEach)
						pending[e.Stamp.Uniq] = swap
						c.output <- &e
					}
					isClean = true
				}
			}

			var ticker <-chan time.Time
			if c.def.triggerEvery > 0 {
				ticker = time.NewTicker(c.def.triggerEvery).C
			}

			resolveAck := func(uniq uint64) {
				if uniq > highestAcked {
					highestAcked = uniq
				}
				buffer := pending[uniq]
				delete(pending, uniq)
				for _, u := range buffer {
					//log.Println("FOLD UPSTREAM ACK ", u)
					c.up.ack(u)
				}
			}

			var stopStamp = uint64(math.MaxUint64)
			var terminated = false

			var doStop = func(uniq uint64) {
				stopStamp = uniq
				maxInputStopStamp := uint64(0)
				for _, x := range pending[uniq] {
					if x > maxInputStopStamp {
						maxInputStopStamp = x
					}
				}
				//log.Println("FOLD UPSTREAM STOP ", maxInputStopStamp)
				c.up.stop(maxInputStopStamp)
			}

			defer c.Close()
			upstream := c.up.output
			checkTerminated := func() {
				if highestAcked >= stopStamp && upstream == nil {
					terminated = true
				}
			}
			for !terminated {
				select {
				case uniq := <-stops:
					doStop(uniq)
					if highestAcked >= stopStamp && upstream == nil {
						terminated = true
					}
				default:
					//select is split into 2 sections to guarantee that the stop signal is processed before any ack
					//because the pseudo-random selection may trigger ack before stop and the pending buffer which
					//must be available for the stop id upstream resolution would be be deleted
					select {

					case uniq := <-stops:
						doStop(uniq)
						checkTerminated()

					case uniq := <-acks:
						resolveAck(uniq)
						if highestAcked >= stopStamp && upstream == nil {
							terminated = true
						}

					case <-ticker:
						trigger()

					case e, ok := <-upstream:
						if !ok {
							trigger()
							upstream = nil
							close(c.output)
							if c.autoi < stopStamp {
								stopStamp = c.autoi
							}
							if highestAcked >= stopStamp && upstream == nil {
								terminated = true
							}
						} else {
							fn.Process(e.Value)
							isClean = false
							buffer = append(buffer, e.Stamp.Uniq)
							if c.def.triggerEach > 0 {
								if len(buffer) >= c.def.triggerEach {
									trigger()
								}
							}
						}
					}
				}
			}
		}()

	default:
		panic(fmt.Errorf("unknown Fn Type: %v", reflect.TypeOf(fn)))
	}
}

func (c *Context) initializeCheckpointer(cap int, pending chan PC, fn Root) {
	var highestPending uint64 //FIXME volatile
	c.Emit = func(element *Element) {
		element.Stamp.Uniq = atomic.AddUint64(&c.autoi, 1)
		highestPending = element.Stamp.Uniq
		element.ack = c.ack
		if element.Stamp.Unix == 0 {
			element.Stamp.Unix = time.Now().Unix()
		}
		if ! c.limitReached(element.Stamp.Uniq) {
			pending <- PC{
				Uniq:           element.Stamp.Uniq,
				Checkpoint:     &element.Checkpoint,
				UpstreamNodeId: element.FromNodeId,
			}
			t := time.NewTimer(30 * time.Second).C //TODO configurable pipeline-level timeout
			for {
				select {
				case c.output <- element:
					return
				case <-t:
					pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
					panic(fmt.Errorf("timeout while trying to push into the root output buffer"))
				}
			}
		}
	}
	acks := make(chan uint64, cap)
	c.ack = func(uniq uint64) {
		if uniq == 0 {
			panic(fmt.Errorf("ack stamp cannot be zero"))
		}
		//acks get nulled when closing but if this was caused by Limit there may still follow some calls to c.ack
		acks <- uniq
	}
	commits := make(chan Watermark, c.def.maxVerticalParallelism*100)
	go func() {
		defer close(commits)
		//commits consumer must run in a separate routine so that the output emitters continue working even if the commit takes a long time, in other words this is one aspect of the backpressure feature
		t := time.NewTicker(250 * time.Millisecond).C
		var checkpoint Watermark
		var final = false
		for {
			select {
			case w := <-commits:
				if len(w) == 0 {
					final = true
				} else if checkpoint == nil {
					checkpoint = w
				} else {
					for x := range w {
						checkpoint[x] = w[x]
					}
				}
			case <-t:
				if checkpoint != nil {
					//c.log("Commit Checkpoint: %v", checkpoint)
					fn.Commit(checkpoint, c)
					checkpoint = nil
				}
				if final {
					//terminating checkpoint
					//c.log("Completed - closing")
					close(pending)
					close(acks)
					acks = nil
					c.close()
					return
				}

			}
		}
	}()

	terminate := make(chan uint64, 1)
	c.Close = func() {
		terminate <- 0
	}
	pendingSuspendable := pending
	pendingCheckpoints := make([]*PC, 0, cap)
	acked := make(map[uint64]bool, cap)
	watermark := make(Watermark)
	var stopStamp uint64 = math.MaxUint64
	c.stop = func(uniq uint64) {
		c.termination <- true
		terminate <- uniq
	}

	go func() {
		terminating := false
		var highestAcked uint64
		resolveAcks := func() {
			for len(pendingCheckpoints) > 0 && acked[pendingCheckpoints[0].Uniq] {
				p := pendingCheckpoints[0]
				watermark[p.Checkpoint.Part] = p.Checkpoint.Data
				pendingCheckpoints = pendingCheckpoints[1:]
				delete(acked, p.Uniq)
				if p.UpstreamNodeId > 0 {
					c.receiver.Ack(p.UpstreamNodeId, p.Uniq)
				} else if c.up != nil {
					c.up.ack(p.Uniq)
				}
			}

			if len(pendingCheckpoints) < cap && pendingSuspendable == nil {
				//release the backpressure after capacity is freed
				pendingSuspendable = pending
			}

			if len(watermark) > 0 {
				commits <- watermark
				watermark = make(map[int]interface{})
			}

			if terminating {
				if highestPending <= highestAcked || stopStamp <= highestAcked {
					if len(watermark) == 0 && len(pendingCheckpoints) == 0 && len(acked) == 0 {
						commits <- Watermark{}
					} else {
						//c.log("Watermark: %v", watermark)
						//c.log("Pending: %v", pendingCheckpoints)
						//c.log("acked: %v", acked)
					}
				}
			}
		}
		for !c.closed {
			select {
			case s := <-terminate:
				terminating = true
				if s > 0 {
					//c.log("ROOT STOP STAMP %v", s)
					stopStamp = s
					//delete all pending checkpoints above stopStamp
					for p := len(pendingCheckpoints) - 1; p >= 0; p-- {
						if pendingCheckpoints[p].Uniq > stopStamp {
							//c.log("DROPPING CHECKPOINT= %v", pendingCheckpoints[p].Uniq)
							pendingCheckpoints = append(pendingCheckpoints[:p], pendingCheckpoints[p+1:]...)
						}
					}
					//TODO delete acked higher than stopStamp
					for a := range acked {
						if a > stopStamp {
							delete(acked, a)
						}
					}
				}
				resolveAcks()
			case uniq, ok := <-acks:
				if ok && uniq <= stopStamp {
					//c.log("ACK %d", uniq)
					if uniq > highestAcked {
						highestAcked = uniq
					}
					acked[uniq] = true
					//c.log("BEFORE RESOLVE ROOT ACK %v -> %v", uniq, acked)
					resolveAcks()
					//c.log("AFTER RESOLVE ROOT ACK %v -> %v", uniq, acked)
					//c.log("AFTER ROOT ACK WATERMARK: %v", watermark)
				}

			case p, ok := <-pendingSuspendable:
				if ok && p.Uniq <= stopStamp {
					pendingCheckpoints = append(pendingCheckpoints, &p)
					resolveAcks()
					if len(pendingCheckpoints) == cap {
						//in order to apply backpressure this channel needs to be set to nil so that the select doesn't enter into a busy loop until resolved
						pendingSuspendable = nil
						//c.log("STAGE[%d] Applying backpressure, pending acks: %d\n", c.stage, len(pendingCheckpoints))
					} else if len(pendingCheckpoints) > cap {
						panic(fmt.Errorf("illegal accumulator state, buffer size higher than %d", cap))
					}
				}
			}
		}
	}()
}

func (c *Context) close() {
	if ! c.closed {
		//c.log("CLOSED")
		c.completed <- true
		c.closed = true
		if fn, ok := c.def.Fn.(Closeable); ok {
			if err := fn.Close(c); err != nil {
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

func (c *Context) limitReached(stopStamp uint64) bool {
	if c.limitEnabled {
		after := atomic.AddUint64(&c.limitCounter, 1)
		if after == c.def.limit {
			if c.stop == nil {
				panic(fmt.Errorf("stage[%d] stop function is nil", c.stage))
			} else {
				//c.log("Limit Reached @ %d", stopStamp)
				c.stop(stopStamp)
			}
		} else if after > c.def.limit {
			return true
		}
	}
	return false
}
