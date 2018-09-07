package goc

import (
	"fmt"
	"log"
	"reflect"
)

type Stream struct {
	Type         reflect.Type
	Materializer func(chan interface{})
	underlying   chan interface{}
	pipeline	 *Pipeline
	transform    Transform
}

type Checkpoint []interface{}

type Transform interface {
	Commit(Checkpoint) error
}


func (stream *Stream) Materialize() {
	log.Printf("Materilaizing Stream of %q \n", stream.Type)
	if stream.underlying != nil {
		panic(fmt.Errorf("stream already materialized"))
	}
	stream.underlying = make(chan interface{})
	go func() {
		defer close(stream.underlying)
		stream.Materializer(stream.underlying)
	}()
}

func (stream *Stream) Commit(checkpoint Checkpoint) error {
	if stream.transform != nil {
		return stream.transform.Commit(checkpoint)
	} else {
		return nil
	}
}

func (stream *Stream) Apply(t Transform) *Stream {
	if method, exists := reflect.TypeOf(t).MethodByName("Fn"); !exists {
		panic(fmt.Errorf("transform must provide Fn method"))
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
			output = stream.Transform(fn.Interface())
		} else {
			output = stream.Map(fn.Interface())
		}
		output.transform = t
		return output
	}

}

func (stream *Stream) Map(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

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

	return stream.pipeline.From(&Stream{
		Type: fnType.Out(0),
		Materializer: func(output chan interface{}) {
			for d := range stream.underlying {
				v := reflect.ValueOf(d)
				r := fnVal.Call([]reflect.Value{v})
				output <- r[0].Interface()
			}
		},
	})

}

func (stream *Stream) Filter(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

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

	return stream.pipeline.From(&Stream{
		Type: fnType.In(0),
		Materializer: func(output chan interface{}) {
			for d := range stream.underlying {
				v := reflect.ValueOf(d)
				if fnVal.Call([]reflect.Value{v})[0].Bool() {
					output <- d
				}
			}
		},
	})
}

func (stream *Stream) Transform(fn interface{}) *Stream {

	fnType := reflect.TypeOf(fn)
	fnVal := reflect.ValueOf(fn)

	if fnType.NumIn() != 2 || fnType.NumOut() != 0 {
		panic(fmt.Errorf("transform func must have zero return values and exatly 2 arguments: input underlying and an output underlying"))
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
		panic(fmt.Errorf("transform func input argument must be a underlying of %q, got underlying of %q", stream.Type, fnType.In(0).Elem()))
	}

	////////////////////////////////////////////////////////////////////////////

	return stream.pipeline.From(&Stream{
		Type: outChannelType.Elem(),
		Materializer: func(output chan interface{}) {
			intermediateIn := reflect.MakeChan(inChannelType, 0)
			go func() {
				defer intermediateIn.Close()
				for d := range stream.underlying {
					intermediateIn.Send(reflect.ValueOf(d))
				}
			}()

			intermediateOut := reflect.MakeChan(outChannelType, 0)
			go func() {
				defer intermediateOut.Close()
				fnVal.Call([]reflect.Value{intermediateIn, intermediateOut})
			}()

			for {
				o, ok := intermediateOut.Recv()
				if !ok {
					return
				} else {
					output <- o.Interface()
				}
			}
		},
	})

}
