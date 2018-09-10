package goc

import (
	"fmt"
	"reflect"
)

type Stream struct {
	Type      reflect.Type
	runner    func(output chan *Element)
	output    chan *Element
	pipeline  *Pipeline
	transform Fn
	closed    bool
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
		for element := range source.output {
			if element.hasSignal() {
				to <- element
			} else {
				interceptedOutput <- element
			}
		}
	}(stream)
	return interceptedOutput
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

	return stream.pipeline.Apply(stream, fnType.Out(0), nil, func(input <-chan *Element, output chan *Element) {
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

	return stream.pipeline.Apply(stream, fnType.In(0), nil, func(input <-chan *Element, output chan *Element) {
		for d := range input {
			v := reflect.ValueOf(d.Value)
			if fnVal.Call([]reflect.Value{v})[0].Bool() {
				output <- d
			}
		}
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

	return stream.pipeline.Apply(stream, outChannelType.Elem(), nil, func(input <-chan *Element, output chan *Element) {
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
