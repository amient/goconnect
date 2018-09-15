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
	streams []*Stream
	stamp   uint32
	coders  []MapFn
}

func NewPipeline(coders []MapFn) *Pipeline {
	return &Pipeline{
		streams: []*Stream{},
		coders: coders,
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
		return p.FlatMap(p.injectCoder(that, fn.InType()), fn)
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
		return p.Map(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		outputElement := fn.Process(input)
		sanitise(outputElement, input)
		output <- outputElement
	})
}

func (p *Pipeline) ForEach(that *Stream, fn ForEachFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ForEach(p.injectCoder(that, fn.InType()), fn)
	} else {
		return p.elementWise(that, ErrorType, fn, func(input *Element, output OutputChannel) {
			fn.Process(input)
		})
	}
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
					if _, is := fn.(TransformFn); is {
						//FIXME
						run(nil, output)
					}
					output <- element
					up.terminate <- true
					return
				case NoSignal:
					if element.Stamp.hi == 0 {
						s := atomic.AddUint32(&p.stamp, 1)
						element.Stamp = Stamp{s, s}
					}
					up.pendingAck(element)
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

func (p *Pipeline) Transform(that *Stream, fn TransformFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Transform(p.injectCoder(that, fn.InType()), fn)
	}
	var stamp Stamp
	var highestTimestamp *time.Time
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		if input == nil {
			//FIXME not the most clever trigger
			for _, outputElement := range fn.Trigger() {
				outputElement.Stamp = stamp
				if outputElement.Timestamp == nil {
					outputElement.Timestamp = highestTimestamp
				}
				output <- outputElement
				stamp.lo = 0
				highestTimestamp = nil
			}
		} else {
			fn.Process(input)
			if highestTimestamp == nil || input.Timestamp.After(*highestTimestamp) {
				highestTimestamp = input.Timestamp
			}
			stamp.hi = input.Stamp.hi
			if stamp.lo == 0 {
				stamp.lo = input.Stamp.lo
			}
		}

	})
}

func (p *Pipeline) Run() {

	log.Printf("Materializing Pipeline of %d stages\n", len(p.streams))

	source := p.streams[0]
	sink := p.streams[len(p.streams)-1]

	for s, stream := range p.streams {
		stream.initialize(s + 1)
		go func(s int, stream *Stream) {
			stream.runner(stream.output)
			//assuming single source
			if stream == source && !stream.terminating {
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
			//assuming single source
			if !source.closed {
				log.Printf("Caught signal %v: Cancelling\n", sig)
				source.output <- &Element{signal: FinalCheckpoint}
			}

		case e, more := <-sink.output:
			if more {
				switch e.signal {
				case NoSignal:
				case FinalCheckpoint:
					//assuming single source await until all pendingAck acks have been completed
					<-source.completed
					for i := len(p.streams) - 1; i >= 0; i-- {
						p.streams[i].close()
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

func (p *Pipeline) injectCoder(that *Stream, to reflect.Type) *Stream {
	var scan func(in reflect.Type, out reflect.Type, d int, chain []MapFn) []MapFn
	scan = func(in reflect.Type, out reflect.Type, d int, chain []MapFn) []MapFn {
		if d <= 5 {
			for _, c := range p.coders {
				if c.InType().AssignableTo(in) && c.OutType().AssignableTo(out) {
					branch := make([]MapFn, len(chain), len(chain)+1)
					copy(branch, chain)
					branch = append(branch, c)
					return branch
				}
			}
			for _, c := range p.coders {
				if c.InType().AssignableTo(in) {
					branch := make([]MapFn, len(chain), len(chain)+1)
					copy(branch, chain)
					branch = append(branch, c)
					return scan(c.OutType(), out, d+1, branch)
				}
			}
		}
		panic(fmt.Errorf("cannot find any coders to satisfy: %v => %v, depth %d", in, out, d))
	}
	log.Printf("Injecting coders to satisfy: %v => %v ", that.Type, to)
	for _, mapper := range scan(that.Type, to, 1, []MapFn{}) {
		log.Printf("Injecting coder: %v => %v ", that.Type, mapper.OutType())
		that = that.Apply(mapper)
	}
	return that
}
