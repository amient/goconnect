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
	"sync/atomic"
	"syscall"
	"time"
)

type Pipeline struct {
	streams    []*Stream
	lastCommit time.Time
	stamp      uint32
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
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		for i, outputElement := range fn.Process(input) {
			sanitise(outputElement, input)
			if outputElement.Checkpoint == nil {
				outputElement.Checkpoint = Checkpoint{0: i}
			}
			output <- outputElement
		}
	})
}

func (p *Pipeline) Map(that *Stream, fn MapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
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
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, ErrorType, fn, func(input *Element, output OutputChannel) {
		fn.Process(input)
		input.Ack()
	})
}

func (p *Pipeline) elementWise(up *Stream, out reflect.Type, fn Fn, run func(input *Element, output OutputChannel)) *Stream {
	return p.register(&Stream{
		Type: out,
		fn:   fn,
		up: up,
		runner: func(output OutputChannel) {
			//var checkpoint = make(Checkpoint)
			for element := range up.output {
				//FIXME merging of checkpoints has to happen in Stream.pending & Stream.ack
				//checkpoint.merge(element.Checkpoint)
				switch element.signal {
				case NoSignal:
					if element.Stamp == 0 {
						element.Stamp = Stamp(atomic.AddUint32(&p.stamp, 1))
					}
					element.ack = func(x Stamp) error {
						return up.ack(x)
					}
					up.pending(element.Stamp, element.Checkpoint)
					if element.Timestamp == nil {
						now := time.Now()
						element.Timestamp = &now
					}
					run(element, output)
				case FinalCheckpoint:
					output <- element
					//up.checkpoint.merge(checkpoint)
					//checkpoint = make(Checkpoint)
					if fn, ok := up.fn.(SideEffect); ok {
						//flush intermediate stages
						fn.Flush()
					}
					if fn, ok := up.fn.(Closeable); ok {
						fn.Close()
					}
				}
			}
		},
	})
}

func (p *Pipeline) Run(/*commitInterval time.Duration*/) {

	log.Printf("Running Pipeline of %d stages\n", len(p.streams))

	for s, stream := range p.streams {
		log.Printf("Materilaizing Stream of %q \n", stream.Type)
		if stream.output != nil {
			panic(fmt.Errorf("stream already materialized"))
		}

		//TODO configurable capacity for checkpoint buffers
		stream._acked = make(map[Stamp]bool, 100)
		stream._pending = make([]Stamp,0, 100)
		stream._checkpoints= make(map[Stamp]Checkpoint, 100)

		stream.output = make(chan *Element)
		go func(s int, stream *Stream) {
			defer log.Printf("Stage terminated[%d]: %q\n", s, stream.Type)
			defer stream.close()
			stream.id = s
			stream.runner(stream.output)
			if !stream.closed {
				//this is here to terminate bounded sources with a commit
				stream.output <- &Element{signal: FinalCheckpoint}
			}
		}(s, stream)

	}

	source := p.streams[0]
	sink := p.streams[len(p.streams)-1]

	//open committer tick underlying
	//committerTick := time.NewTicker(commitInterval).C

	//open termination signal underlying
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigterm:
			log.Printf("Caught signal %v: Cancelling\n", sig)
			if !source.closed {
				source.output <- &Element{signal: FinalCheckpoint}
			}

		//case timestamp := <-committerTick:
		//	if timestamp.Sub(p.lastCommit) > commitInterval {
		//		//log.Println("Start Commit")
		//		if !source.closed {
		//			source.output <- &Element{signal: FinalCheckpoint}
		//		}
		//		p.lastCommit = timestamp
		//	}

		case e, more := <-sink.output:
			if !more {
				//FIXME pipeline cannot terminate until all flushed data is acked
				log.Println("Pipeline Terminated")
				for i := len(p.streams) - 1; i >= 0; i-- {
					stream := p.streams[i]
					if fn, ok := stream.fn.(Closeable); ok {
						if err := fn.Close(); err != nil {
							panic(err)
						}
					}
				}
				//this is the only place that exits the for-select which in turn is only possible by FinalCheckpoint signal below
				return
			}
			switch e.signal {
			case NoSignal:
			case FinalCheckpoint:
				//flush and close the final stage
				if fn, ok := sink.fn.(SideEffect); ok {
					fn.Flush()
				}
				if fn, ok := sink.fn.(Closeable); ok {
					fn.Close()
				}

			}


		}

	}
}

