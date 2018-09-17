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
	stamp   uint64
	coders  []MapFn
}

func NewPipeline(coders []MapFn) *Pipeline {
	return &Pipeline{
		streams: []*Stream{},
		coders:  coders,
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
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output Channel) {
		for i, outputElement := range fn.Process(input) {
			outputElement.Stamp = input.Stamp
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
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output Channel) {
		outputElement := fn.Process(input)
		outputElement.Stamp = input.Stamp
		output <- outputElement
	})
}

func (p *Pipeline) ForEach(that *Stream, fn ForEachFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ForEach(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, ErrorType, fn, func(input *Element, output Channel) {
		fn.Process(input)
	})
}

func (p *Pipeline) Transform(that *Stream, fn TransformFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Transform(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output Channel) {
		fn.Process(input)
	})
}

func (p *Pipeline) elementWise(up *Stream, out reflect.Type, fn Fn, run func(input *Element, output Channel)) *Stream {
	return p.register(&Stream{
		Type: out,
		fn:   fn,
		up:   up,
		runner: func(output Channel) {
			var stamp Stamp
			for element := range up.output {

				switch element.signal {
				case FinalCheckpoint:
					if t, is := fn.(TransformFn); is {
						//FIXME triggering has to be possible in other ways
						for _, outputElement := range t.Trigger() {
							outputElement.Stamp = stamp
							output <- outputElement
							stamp.Lo = 0 //make it invalid
						}
					}
					output <- element
					up.terminate <- true
					return
				case NoSignal:
					//initial stamping of elements
					if element.Stamp.Hi == 0 {
						s := atomic.AddUint64(&p.stamp, 1)
						element.Stamp.Hi = s
						element.Stamp.Lo = s
					}
					if element.Stamp.Time == (time.Time{}) {
						element.Stamp.Time = time.Now()
					}
					up.pendingAck(element)

					//merged stamp is used forTransform.Trigger()
					stamp.merge(element.Stamp)

					//pass the element after it has been intercepted to the fn
					run(element, output)
				}
			}
		},
	})
}

func (p *Pipeline) Run() {

	log.Printf("Materializing Pipeline of %d stages\n", len(p.streams))

	source := p.streams[0]
	sink := p.streams[len(p.streams)-1]
	var start = time.Now()
	stopwatch := func() {
		log.Printf("Executiong took %f ms", time.Now().Sub(start).Seconds()*1000)
	}
	defer stopwatch()

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
					log.Printf("Output completion took %f ms", time.Now().Sub(start).Seconds()*1000)
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
