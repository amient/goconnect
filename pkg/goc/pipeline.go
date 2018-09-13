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
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Pipeline struct {
	streams []*Stream
	stamp   uint32
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		streams: []*Stream{},
	}
}

func (p *Pipeline) register(stream *Stream) *Stream {
	stream.pipeline = p
	p.streams = append(p.streams, stream)
	return stream
}

func sanitise(out *Element, in *Element) {
	out.Stamp = in.Stamp
	if out.Timestamp == nil {
		out.Timestamp = in.Timestamp
	}
}

func (p *Pipeline) Root(source RootFn) *Stream {
	return p.register(&Stream{
		Type:   source.OutType(),
		fn:     source,
		runner: source.Run,
	})
}

func (p *Pipeline) FlatMap(that *Stream, fn FlatMapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply process with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		for i, outputElement := range fn.Process(input) {
			sanitise(outputElement, input)
			if outputElement.Checkpoint.Data == nil {
				outputElement.Checkpoint = Checkpoint{Part: 0, Data: i}
			}
			output <- outputElement
		}
	})
}

func (p *Pipeline) Map(that *Stream, fn MapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply process with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		outputElement := fn.Process(input)
		sanitise(outputElement, input)
		output <- outputElement
	})
}

func (p *Pipeline) ForEach(that *Stream, fn ForEachFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply process with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, ErrorType, fn, func(input *Element, output OutputChannel) {
		fn.Process(input)
	})
}

func (p *Pipeline) elementWise(up *Stream, out reflect.Type, fn Fn, run func(input *Element, output OutputChannel)) *Stream {
	return p.register(&Stream{
		Type: out,
		fn:   fn,
		up:   up,
		runner: func(output OutputChannel) {
			for element := range up.output {
				switch element.signal {
				case FinalCheckpoint:
					output <- element
				case NoSignal:
					if element.Stamp == 0 {
						element.Stamp = Stamp(atomic.AddUint32(&p.stamp, 1))
					}
					element.ack = func(x Stamp) error {
						//TODO aggregate multiple acks per period of time and use channel
						return up.ack(x)
					}
					up.pending(element)
					if element.Timestamp == nil {
						now := time.Now()
						element.Timestamp = &now
					}
					run(element, output)
				}
			}
		},
	})
}

func (p *Pipeline) Run( /*commitInterval time.Duration*/) {

	log.Printf("Running Pipeline of %d stages\n", len(p.streams))

	source := p.streams[0]
	sink := p.streams[len(p.streams)-1]

	for s, stream := range p.streams {
		log.Printf("Materilaizing Stream of %q \n", stream.Type)
		if stream.output != nil {
			panic(fmt.Errorf("stream already materialized"))
		}
		stream._lock = sync.RWMutex{}
		//TODO configurable capacity for checkpoint buffers
		stream._cap = 100
		stream._acked = make(map[Stamp]bool, stream._cap)
		stream._pending = make(map[int][]Stamp, 10)
		stream._checkpoints = make(map[Stamp]interface{}, stream._cap)
		stream.termination = make(chan bool, 1)
		stream.output = make(chan *Element)
		go func(s int, stream *Stream) {
			stream.stage = s + 1
			stream.runner(stream.output)
			//assuming single source
			if stream == source && !stream.closed {
				//this is here to terminate bounded sources with a commit
				stream.output <- &Element{signal: FinalCheckpoint}
			}
		}(s, stream)

	}

	//open termination signal underlying
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigterm:
			log.Printf("Caught signal %v: Cancelling\n", sig)
			//assuming single source
			if !source.closed {
				source.output <- &Element{signal: FinalCheckpoint}
			}

		case e, more := <-sink.output:
			if more {
				switch e.signal {
				case NoSignal:
				case FinalCheckpoint:
					for i := len(p.streams) - 1; i >= 0; i-- {
						p.streams[i].close()
					}
					//assuming single source, await until all pending acks have been completed
					if !source.clean() {
						log.Println("Waiting for source pending acks")
						<- source.termination
					}
				}
			} else {
				//this is the only place that exits the for-select
				//it will be triggerred by closing of the streams in the above case FinalCheckpoint
				//FinalCheckpoint is injected either by capture sigterm or at the end of bounded
				return
			}



		}

	}
}
