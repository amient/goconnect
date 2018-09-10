package goc

import (
	"fmt"
	"reflect"
)

type Stream struct {
	Type       reflect.Type
	output     chan *Element
	runner     func(output chan *Element)
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

func (stream *Stream) forward(to chan *Element) chan *Element {
	interceptedOutput := make(chan *Element)
	go func(source *Stream) {
		defer close(interceptedOutput)
		var checkpoint = make(Checkpoint)
		for element := range source.output {
			switch element.signal {
			case NoSignal:
				if element.Checkpoint != nil {
					for k, v := range element.Checkpoint {
						checkpoint[k] = v
					}
				}
				interceptedOutput <- element
			case ControlDrain:
				to <- element
				for k, v := range checkpoint {
					stream.checkpoint[k] = v
				}
				checkpoint = make(Checkpoint)
				if fn, ok := stream.fn.(SideEffect); ok {
					fn.Flush(&stream.checkpoint)
				}
			}
		}
	}(stream)
	return interceptedOutput
}

func (stream *Stream) commit() bool {
	if fn, ok := stream.fn.(Commitable); ok {
		if err := fn.Commit(&stream.checkpoint); err != nil {
			panic(err)
		}
		return true
	} else {
		return false
	}
}

func (stream *Stream) Apply(f Fn) *Stream {
	switch fn := f.(type) {
	case TransformFn:
		return stream.pipeline.Transform(stream, fn)
	case DoFn:
		return stream.pipeline.Do(stream, fn)
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
		panic(fmt.Errorf("cannot us Map func with input type %q to consume stream of type %q", fnType.In(0), stream.Type))
	}

	if fnType.NumOut() != 1 {
		panic(fmt.Errorf("map func must have exactly 1 output"))
	}

	return stream.pipeline.apply(stream, fnType.Out(0), nil, func(input <-chan *Element, output chan *Element) {
		for inputElement := range input {
			v := reflect.ValueOf(inputElement.Value)
			r := fnVal.Call([]reflect.Value{v})
			output <- &Element{
				Value:     r[0].Interface(),
				Timestamp: inputElement.Timestamp,
			}
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

	return stream.pipeline.apply(stream, fnType.In(0), nil, func(input <-chan *Element, output chan *Element) {
		for d := range input {
			v := reflect.ValueOf(d.Value)
			if fnVal.Call([]reflect.Value{v})[0].Bool() {
				output <- d
			}
		}
	})

}

func (stream *Stream) Transform(f interface{}) *Stream {

	fnType := reflect.TypeOf(f)
	fnVal := reflect.ValueOf(f)

	if fnType.NumIn() != 2 || fnType.NumOut() != 0 {
		panic(fmt.Errorf("sideEffect func must have zero return values and exatly 2 arguments: input underlying and an output underlying"))
	}

	inChannelType := fnType.In(0)
	outChannelType := fnType.In(1)
	if inChannelType.Kind() != reflect.Chan {
		panic(fmt.Errorf("sideEffect func type input argument must be a chnnel"))
	}

	if outChannelType.Kind() != reflect.Chan {
		panic(fmt.Errorf("sideEffect func type output argument must be a chnnel"))
	}

	//TODO this check will deffered on after network and type coders injection
	if !stream.Type.AssignableTo(inChannelType.Elem()) {
		panic(fmt.Errorf("sideEffect func input argument must be a underlying of %q, got underlying of %q", stream.Type, fnType.In(0).Elem()))
	}

	return stream.pipeline.apply(stream, outChannelType.Elem(), nil, func(input <-chan *Element, output chan *Element) {
		intermediateIn := reflect.MakeChan(inChannelType, 0)
		go func() {
			defer intermediateIn.Close()
			for d := range input {
				intermediateIn.Send(reflect.ValueOf(d.Value))
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
				output <- &Element{Value: o.Interface()}
			}
		}
	})

}
