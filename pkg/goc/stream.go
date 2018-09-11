package goc

import (
	"fmt"
	"reflect"
)

type Stream struct {
	Type       reflect.Type
	output     OutputChannel
	runner     func(output OutputChannel)
	pipeline   *Pipeline
	closed     bool
	checkpoint Checkpoint
	fn         Fn
}

func (stream *Stream) close() {
	if ! stream.closed {
		close(stream.output)
		stream.closed = true
	}
}

func (stream *Stream) commit() bool {
	if fn, ok := stream.fn.(Commitable); ok {
		if err := fn.Commit(stream.checkpoint); err != nil {
			panic(err)
		}
		return true
	} else {
		return false
	}
}

func (stream *Stream) Apply(f Fn) *Stream {
	switch fn := f.(type) {
	//case ElementWiseFn:
	//	return stream.pipeline.ElementWise(stream, fn)
	case FlatMapFn:
		return stream.pipeline.FlatMap(stream, fn)
	case MapFn:
		return stream.pipeline.Map(stream, fn)
	case ForEachFn:
		return stream.pipeline.ForEach(stream, fn)
		//case TransformFn:
		//	return stream.pipeline.Transform(stream, fn)
	default:
		panic(fmt.Errorf("not implemented Apply of ", reflect.TypeOf(fn)))

	}
}

func (stream *Stream) Map(f interface{}) *Stream {

	fnType := reflect.TypeOf(f)
	fnVal := reflect.ValueOf(f)

	if fnType.NumIn() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 input argument"))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(fnType.In(0)) {
		panic(fmt.Errorf("cannot use Map func with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}

	if fnType.NumOut() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 output"))
	}

	return stream.pipeline.elementWise(stream, fnType.Out(0), nil, func(input *Element, output OutputChannel) {
		v := reflect.ValueOf(input.Value)
		r := fnVal.Call([]reflect.Value{v})
		output <- &Element{
			Value: r[0].Interface(),
			Timestamp: input.Timestamp,
		}
	})

}

func (stream *Stream) Filter(f interface{}) *Stream {

	fnType := reflect.TypeOf(f)
	fnVal := reflect.ValueOf(f)

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

	return stream.pipeline.elementWise(stream, fnType.In(0), nil, func(input *Element, output OutputChannel) {
		v := reflect.ValueOf(input.Value)
		if fnVal.Call([]reflect.Value{v})[0].Bool() {
			output <- input
		}
	})

}

//func (stream *Stream) Transform(f interface{}) *Stream {
//
//	fnType := reflect.TypeOf(f)
//	fnVal := reflect.ValueOf(f)
//
//	if fnType.NumIn() != 2 || fnType.NumOut() != 0 {
//		panic(fmt.Errorf("sideEffect func must have zero return values and exatly 2 arguments: input underlying and an output underlying"))
//	}
//
//	inChannelType := fnType.In(0)
//	outChannelType := fnType.In(1)
//	if inChannelType.Kind() != reflect.Chan {
//		panic(fmt.Errorf("sideEffect func type input argument must be a chnnel"))
//	}
//
//	if outChannelType.Kind() != reflect.Chan {
//		panic(fmt.Errorf("sideEffect func type output argument must be a chnnel"))
//	}
//
//	//TODO this check will deffered on after network and type coders injection
//	if !stream.Type.AssignableTo(inChannelType.Elem()) {
//		panic(fmt.Errorf("sideEffect func input argument must be a underlying of %q, got underlying of %q", stream.Type, fnType.In(0).Elem()))
//	}
//
//	return stream.pipeline.group(stream, outChannelType.Elem(), nil, func(input InputChannel, output OutputChannel) {
//		intermediateIn := reflect.MakeChan(inChannelType, 0)
//		go func() {
//			defer intermediateIn.Close()
//			for d := range input {
//				intermediateIn.Send(reflect.ValueOf(d.Value))
//			}
//		}()
//
//		intermediateOut := reflect.MakeChan(outChannelType, 0)
//		go func() {
//			defer intermediateOut.Close()
//			fnVal.Call([]reflect.Value{intermediateIn, intermediateOut})
//		}()
//
//		for {
//			o, ok := intermediateOut.Recv()
//			if !ok {
//				return
//			} else {
//				output <- &Element{Value: o.Interface()}
//			}
//		}
//	})
//
//}
