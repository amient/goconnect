package goc

import (
	"fmt"
	"log"
	"reflect"
)

type Stream struct {
	Type      reflect.Type
	Channel   chan interface{}
	generator *Fn
	transform Transform
}

func (stream *Stream) Materialize() {
	log.Printf("Materilaizing Stream of %q \n", stream.Type)
	if stream.generator != nil {
		stream.generator.materialize()
	}
}

func (stream *Stream) Flush() error {
	if stream.transform != nil {
		return stream.transform.Flush()
	} else {
		return nil
	}
}

func (stream *Stream) Apply(t Transform) *Stream {
	if method, exists := reflect.TypeOf(t).MethodByName("Fn"); !exists {
		panic(fmt.Errorf("transform must provide Fn method: %q", method))
	} else {
		v := reflect.ValueOf(t)
		args := make([]reflect.Type, method.Type.NumIn()-1)
		for i := 1; i < method.Type.NumIn(); i++ {
			args[i-1] = method.Type.In(i)
		}
		ret := make([]reflect.Type, method.Type.NumOut())
		for i := 0; i < method.Type.NumOut(); i++ {
			ret[i] = method.Type.Out(i)
		}
		fn := reflect.MakeFunc(reflect.FuncOf(args, ret, false), func(args []reflect.Value) (results []reflect.Value) {
			methodArgs := append([]reflect.Value{v}, args...)
			return method.Func.Call(methodArgs)
		})

		var output *Stream
		if len(ret) > 1 {
			panic(fmt.Errorf("transform must have 0 or 1 return value"))
		} else if len(ret) == 0 {
			output = stream.Transform(fn)
		} else {
			output = stream.Map(fn.Interface())
		}
		output.transform = t
		return output
	}

}

func (stream *Stream) Map(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)

	if fnType.NumIn() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 input argument"))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(fnType.In(0)) {
		panic(fmt.Errorf("cannot us Map func with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}

	if fnType.NumOut() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 output"))
	}

	////////////////////////////////////////////////////////////////////////////

	f := &Fn{
		up:    stream,
		FnVal: reflect.ValueOf(fn),
		FnTyp: fnType,
	}

	f.out = &Stream{
		generator: f,
		Channel:   make(chan interface{}),
		Type:      fnType.Out(0),
	}

	f.mat = func() {
		stream.Materialize()
		for d := range stream.Channel {
			v := reflect.ValueOf(d)
			r := f.FnVal.Call([]reflect.Value{v})
			f.out.Channel <- r[0].Interface()
		}
	}

	return f.out
}

func (stream *Stream) Filter(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)

	if fnType.NumIn() != 1 {
		panic(fmt.Errorf("filter func must have exactly 1 input argument of type %q", stream.Type))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(fnType.In(0)) {
		panic(fmt.Errorf("cannot us FnEW with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}
	if fnType.NumOut() != 1 || fnType.Out(0).Kind() != reflect.Bool {
		panic(fmt.Errorf("FnVal must have exactly 1 output and it should be bool"))
	}

	////////////////////////////////////////////////////////////////////////////

	f := &Fn{
		up:    stream,
		FnVal: reflect.ValueOf(fn),
		FnTyp: fnType,
	}

	f.out = &Stream{
		generator: f,
		Channel:   make(chan interface{}),
		Type:      fnType.In(0),
	}

	f.mat = func() {
		stream.Materialize()
		for d := range stream.Channel {
			v := reflect.ValueOf(d)
			if f.FnVal.Call([]reflect.Value{v})[0].Bool() {
				f.out.Channel <- d
			}
		}
	}

	return f.out
}

func (stream *Stream) Transform(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)

	if fnType.NumIn() != 2 || fnType.NumOut() != 0 {
		panic(fmt.Errorf("transform func must have zero return values and exatly 2 arguments: input Channel and an output Channel"))
	}

	inChannelType := fnType.In(0)
	outChannelType := fnType.In(1)
	if inChannelType.Kind() != reflect.Chan {
		panic(fmt.Errorf("transform func type input argument must be a chnnel"))
	}

	if outChannelType.Kind() != reflect.Chan {
		panic(fmt.Errorf("transform func type output argument must be a chnnel"))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(inChannelType.Elem()) {
		panic(fmt.Errorf("transform func input argument must be a Channel of %q, got Channel of %q", stream.Type, fnType.In(0).Elem()))
	}

	////////////////////////////////////////////////////////////////////////////

	f := &Fn{
		up:    stream,
		FnVal: reflect.ValueOf(fn),
		FnTyp: fnType,
	}

	f.out = &Stream{
		Type:      outChannelType.Elem(),
		Channel:   make(chan interface{}),
		generator: f,
	}

	f.mat = func() {
		intermediateIn := reflect.MakeChan(inChannelType, 0)
		go func() {
			stream.Materialize()
			defer intermediateIn.Close()
			for d := range stream.Channel {
				intermediateIn.Send(reflect.ValueOf(d))
			}
		}()

		intermediateOut := reflect.MakeChan(outChannelType, 0)
		go func() {
			defer intermediateOut.Close()
			f.FnVal.Call([]reflect.Value{intermediateIn, intermediateOut})
		}()

		for {
			o, ok := intermediateOut.Recv()
			if !ok {
				return
			} else {
				f.out.Channel <- o.Interface()
			}
		}

	}

	return f.out

}
