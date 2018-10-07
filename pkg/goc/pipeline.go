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

func (p *Pipeline) WithCoders(coders []MapFn, defaultPar int) *Pipeline {
	p.coders = coders
	p.defaultCoderPar = defaultPar
	return p
}

func (p *Pipeline) Root(source Root) *Def {
	return p.register(&Def{Type: source.OutType(), Fn: source})
}

func (p *Pipeline) Transform(that *Def, fn Transform) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Transform(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Up: that, Type: fn.OutType(), Fn: fn})
}

func (p *Pipeline) ForEach(that *Def, fn ForEach) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ForEach(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Up: that, Type: ErrorType, Fn: fn})
}

func (p *Pipeline) FlatMap(that *Def, fn FlatMapFn) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.FlatMap(p.injectCoder(that, fn.InType()), fn)
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

func (p *Pipeline) ForEachFn(that *Def, fn ForEachFn) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.ForEachFn(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: ErrorType, Fn: fn, Up: that})
}

func (p *Pipeline) Group(that *Def, fn GroupFn) *Def {
	if !that.Type.AssignableTo(fn.InType()) {
		return p.Group(p.injectCoder(that, fn.InType()), fn)
	}
	return p.register(&Def{Type: fn.OutType(), Fn: fn, Up: that})
}

func (p *Pipeline) register(def *Def) *Def {
	def.pipeline = p
	def.Id = len(p.Defs)
	def.maxVerticalParallelism = 1
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

func (p *Pipeline) Apply(def *Def, f Fn) *Def {
	switch fn := f.(type) {
	case GroupFn:
		return p.Group(def, fn)
	case ForEachFn:
		return p.ForEachFn(def, fn)
	case MapFn:
		return p.Map(def, fn)
	case FilterFn:
		return p.Filter(def, fn)
	case FlatMapFn:
		return p.FlatMap(def, fn)
	case Transform:
		return p.Transform(def, fn)
	case ForEach:
		return p.ForEach(def, fn)
	default:
		panic(fmt.Errorf("only on of the interfaces defined in goc/Fn.go can be applied"))
		panic(fmt.Errorf("reflective transforms need: a) def.Group to have guaranteees and b) perf-tested, %v", reflect.TypeOf(f)))
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
		//	var output *Def
		//	if len(ret) > 1 {
		//		panic(fmt.Errorf("transform must have 0 or 1 return value"))
		//	} else if len(ret) == 0 {
		//		output = def.Group(Fn)
		//	} else {
		//		output = def.Map(Fn.Interface())
		//	}
		//	output.Fn = f
		//	return output
		//}

	}

}