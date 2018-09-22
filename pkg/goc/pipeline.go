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
	Streams []*Stream
	stamp   uint64
	coders  []MapFn
}

func NewPipeline(coders []MapFn) *Pipeline {
	return &Pipeline{
		Streams: []*Stream{},
		coders:  coders,
	}
}

func (p *Pipeline) Root(source RootFn) *Stream {
	return p.register(&Stream{
		Type:   source.OutType(),
		Fn:     source,
		runner: source.Run,
	})
}

func (p *Pipeline) Transform(that *Stream, fn TransformFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Transform(p.injectCoder(that, fn.InType()), fn)
	}
	return p.transform(that, fn.OutType(), fn , fn.Run)
}


func (p *Pipeline) FlatMap(that *Stream, fn FlatMapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.FlatMap(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output chan *Element) {
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
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output chan *Element) {
		outputElement := fn.Process(input)
		outputElement.Stamp = input.Stamp
		output <- outputElement
	})
}

func (p *Pipeline) UserMap(that *Stream, fn interface{}) *Stream {
	t := reflect.TypeOf(fn)
	inType := t.In(0)
	if !that.Type.AssignableTo(inType) {
		return p.UserMap(p.injectCoder(that, inType), fn)
	}
	v := reflect.ValueOf(fn)
	return p.elementWise(that, t.Out(0), fn, func(input *Element, output chan *Element) {
		output <- &Element{
			Stamp: input.Stamp,
			Value: v.Call([]reflect.Value{reflect.ValueOf(input.Value)})[0].Interface(),
		}
	})


}

func (p *Pipeline) ForEach(that *Stream, fn ForEachFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ForEach(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, ErrorType, fn, func(input *Element, output chan *Element) {
		fn.Process(input)
	})
}

func (p *Pipeline) Group(that *Stream, fn GroupFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Group(p.injectCoder(that, fn.InType()), fn)
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output chan *Element) {
		fn.Process(input)
	})
}

func (p *Pipeline) transform(up *Stream, out reflect.Type, fn Fn, run func(input <- chan *Element, context *Context)) *Stream {
	return p.register(&Stream{
		Type: out,
		Fn:   fn,
		Up:   up,
		runner: func(output chan *Element) {
			defer close(output)
			//TODO maybe
		},
	})
}

func (p *Pipeline) elementWise(up *Stream, out reflect.Type, fn Fn, run func(input *Element, output chan *Element)) *Stream {
	return p.register(&Stream{
		Type: out,
		Fn:   fn,
		Up:   up,
		runner: func(output chan *Element) {
			defer close(output)
			groupFn, isGroupFn := fn.(GroupFn)
			var highStamp Stamp
			trigger := make(chan bool, 10) //FIXME not sure why this has to be 10 but it seams that trigger <- doesn't guarantee to be selected next
			for {
				select {
				//TODO trigger <- false based on the Fn's trigger channel
				case terminated := <- trigger:
					if isGroupFn {
						for _, element := range groupFn.Trigger() {
							element.Stamp = highStamp
							output <- element
							highStamp.Lo = 0 //make it invalid
						}
					}
					if terminated {
						return
					}
				case element, more := <-up.output:
					if !more {
						trigger <- true
					} else {
						run(element, output)
						//merged high highStamp for triggers
						highStamp.Merge(element.Stamp)
					}
				}
			}

		},
	})
}

func (p *Pipeline) Run() {

	log.Printf("Materializing Pipeline of %d stages\n", len(p.Streams))

	source := p.Streams[0]

	var start = time.Now()
	stopwatch := func() {
		log.Printf("Executiong took %f ms", time.Now().Sub(start).Seconds()*1000)
	}
	defer stopwatch()

	for s, stream := range p.Streams {
		stream.initialize(s + 1)

		intercept := make(chan *Element, 1)
		go func(stream *Stream) {
			stream.runner(intercept)
		}(stream)

		stream.output = make(chan *Element, 100)
		go func(stream *Stream, triggers chan bool) {

			for element := range intercept {
				//initial stamping of elements
				if element.Stamp.Hi == 0 {
					s := atomic.AddUint64(&p.stamp, 1)
					element.Stamp.Hi = s
					element.Stamp.Lo = s
				}
				if element.Stamp.Unix == 0 {
					element.Stamp.Unix = time.Now().Unix()
				}
				//checkpointing
				stream.pendingAck(element)

				//output
				stream.output <- element
			}

			close(stream.output)
			stream.terminate <- true
		}(stream, make(chan bool, 1))
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
				source.terminate <- true
			}
		case <-source.completed:
			for i := len(p.Streams) - 1; i >= 0; i-- {
				p.Streams[i].close()
			}
			return
		}
	}

}

func (p *Pipeline) register(stream *Stream) *Stream {
	stream.pipeline = p
	stream.Id = len(p.Streams)
	p.Streams = append(p.Streams, stream)
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
	//log.Printf("Injecting coders to satisfy: %v => %v ", that.Type, to)
	for _, mapper := range scan(that.Type, to, 1, []MapFn{}) {
		log.Printf("Injecting coder: %v => %v ", that.Type, mapper.OutType())
		that = that.Apply(mapper)
	}
	return that
}

func (p *Pipeline) Apply(stream *Stream, f Fn) *Stream {
	switch fn := f.(type) {
	case GroupFn:
		return p.Group(stream, fn)
	case ForEachFn:
		return p.ForEach(stream, fn)
	case MapFn:
		return p.Map(stream, fn)
	case FlatMapFn:
		return p.FlatMap(stream, fn)
	case TransformFn:
		return p.Transform(stream ,fn)
	default:
		t := reflect.TypeOf(fn)
		if t.Kind() == reflect.Func && t.NumIn() == 1 && t.NumOut() == 1 {
			return p.UserMap(stream ,f)
		} else {
			panic(fmt.Errorf("only on of the interfaces defined in goc/Fn.go  can be applied"))
		}

		panic(fmt.Errorf("reflective transforms need: a) stream.Group to have guaranteees and b) perf-tested, %v", reflect.TypeOf(f)))
		//if method, exists := reflect.TypeOf(f).MethodByName("process"); !exists {
		//	panic(fmt.Errorf("transform must provide process method"))
		//} else {
		//	v := reflect.ValueOf(f)
		//	args := make([]reflect.Type, method.Type.NumIn()-1)
		//	for i := 1; i < method.Type.NumIn(); i++ {
		//		args[i-1] = method.Type.In(i)
		//	}
		//	ret := make([]reflect.Type, method.Type.NumOut())
		//	for i := 0; i < method.Type.NumOut(); i++ {
		//		ret[i] = method.Type.Out(i)
		//	}
		//	Fn := reflect.MakeFunc(reflect.FuncOf(args, ret, false), func(args []reflect.Data) (results []reflect.Data) {
		//		methodArgs := append([]reflect.Data{v}, args...)
		//		return method.Func.Call(methodArgs)
		//	})
		//
		//	var output *Stream
		//	if len(ret) > 1 {
		//		panic(fmt.Errorf("transform must have 0 or 1 return value"))
		//	} else if len(ret) == 0 {
		//		output = stream.Group(Fn)
		//	} else {
		//		output = stream.Map(Fn.Interface())
		//	}
		//	output.Fn = f
		//	return output
		//}

	}

}
