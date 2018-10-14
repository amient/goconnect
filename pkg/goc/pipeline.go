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
	"reflect"
	"time"
)

type Pipeline struct {
	Defs            []*Def
	defaultCoderPar int
	stamp           uint64
	coders          []MapFn
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		Defs:   []*Def{},
	}
}

func (p *Pipeline) Run() {
	graph := ConnectStages(nil, p)
	start := time.Now()
	log.Println("Running a single")
	RunGraphs(graph)
	log.Printf("All stages completed in %f0.0 s", time.Now().Sub(start).Seconds())
}



func (p *Pipeline) WithCoders(coders []MapFn, defaultPar int) *Pipeline {
	p.coders = coders
	p.defaultCoderPar = defaultPar
	return p
}

func (p *Pipeline) Root(source Root) *Def {
	return p.register(&Def{Type: source.OutType(), Fn: source})
}


func (p *Pipeline) Apply(def *Def, f Fn) *Def {
	switch fn := f.(type) {
	case Transform:
		return p.Transform(def, fn)
	case ElementWise:
		return p.ElementWise(def, fn)
	case Sink:
		return p.Sink(def, fn)
	case FoldFn:
		return p.Fold(def, fn)
	case MapFn:
		return p.Map(def, fn)
	case FilterFn:
		return p.Filter(def, fn)
	default:
		panic(fmt.Errorf("only one of the interfaces defined in goc/Fn.go can be applied"))
	}
}

func (p *Pipeline) Transform(that *Def, fn Transform) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Transform(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Up: that, Type: fn.OutType(), Fn: fn})
}

func (p *Pipeline) ElementWise(that *Def, fn ElementWise) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ElementWise(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: fn.OutType(), Fn: fn, Up: that})
}

func (p *Pipeline) Map(that *Def, fn MapFn) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Map(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: fn.OutType(), Fn: fn, Up: that})
}

func (p *Pipeline) Filter(that *Def, fn FilterFn) *Def {
	if !that.Type.AssignableTo(fn.Type()) {
		return p.Filter(p.injectCoder(that, fn.Type()), fn)
	}

	return p.register(&Def{Type: that.Type, Fn: fn, Up: that})
}

func (p *Pipeline) Fold(that *Def, fn FoldFn) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Fold(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: fn.OutType(), Fn: fn, Up: that})
}

func (p *Pipeline) Sink(that *Def, fn Sink) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Sink(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: ErrorType, Fn: fn, Up: that})
}


func (p *Pipeline) register(def *Def) *Def {
	def.pipeline = p
	def.Id = len(p.Defs)
	//defaults
	def.bufferCap = 32
	def.maxVerticalParallelism = 1
	def.triggerEach = 0
	def.triggerEvery = 0
	//
	p.Defs = append(p.Defs, def)
	return def
}

func (p *Pipeline) injectCoder(that *Def, to reflect.Type) *Def {
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
		log.Printf("Injecting coder: %v => %v using default parallelism %d", that.Type, mapper.OutType(), p.defaultCoderPar)
		that = that.Apply(mapper).Par(p.defaultCoderPar)
	}
	return that
}

