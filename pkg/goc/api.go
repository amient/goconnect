package goc


import (
	"fmt"
	"log"
	"reflect"
	"sync"
)







type Stream struct {
	C  chan interface{}
	T  reflect.Type
	up Transform
}

func (s *Stream) Materialize() <-chan interface{} {

	log.Printf("Materilaizing Stream of %q \n", s.T)
	if s.up != nil {
		s.up.materialize()
	}
	return s.C
}

func (s *Stream) Apply(t Transform) *Stream {
	t.input(s)
	return t.output()
}

func FromList(list interface{}) Stream {
	val := reflect.ValueOf(list)
	result := Stream{
		C: make(chan interface{}),
		T: reflect.TypeOf(list).Elem(),
	}
	go func() {
		for i := 0; i < val.Len(); i++ {
			result.C <- val.Index(i).Interface()
		}
		log.Println("Closing Root List")
		close(result.C)

	}()
	return result
}









type Transform interface {
	input(s *Stream)
	output() *Stream
	materialize()
}








func Agg(fn interface{}) Transform {
	return &AggTransform{
		FnVal: reflect.ValueOf(fn),
		FnTyp: reflect.TypeOf(fn),
	}
}

type AggTransform struct {
	up    *Stream
	out   *Stream
	FnVal reflect.Value
	FnTyp reflect.Type
}

func (fn *AggTransform) input(up *Stream) {
	fn.up = up
	if fn.FnTyp.NumIn() != 2 || fn.FnTyp.NumOut() != 0 {
		panic(fmt.Errorf("Aggregation Fn must have zero return values and exatly 2 arguments: input channel and an output channel"))
	}
	inType := fn.FnTyp.In(0)
	outType := fn.FnTyp.In(1)
	if inType.Kind() != reflect.Chan{
		panic(fmt.Errorf("Aggregation Fn type input argument be a chnnel"))
	}
	if !up.T.AssignableTo(inType.Elem()) {
		panic(fmt.Errorf("Aggregation Fn input argument must be a Channel of %q, got Channel of %q", up.T, fn.FnTyp.In(0).Elem()))
	}
	if outType.Kind() != reflect.Chan{
		panic(fmt.Errorf("Aggregation Fn type output argument must be a chnnel"))
	}
}

func (fn *AggTransform) output() *Stream {
	fn.out = &Stream{
		up: fn,
		C:  make(chan interface{}),
		T:  fn.FnTyp.In(1).Elem(),
	}
	return fn.out
}

func (fn *AggTransform) materialize() {
	intermediateIn := reflect.MakeChan(fn.FnTyp.In(0), 0)
	go func() {
		defer intermediateIn.Close()
		for d := range fn.up.Materialize() {
			intermediateIn.Send(reflect.ValueOf(d))
		}
	}()

	intermediateOut := reflect.MakeChan(fn.FnTyp.In(1), 0)
	go func() {
		defer intermediateOut.Close()
		fn.FnVal.Call([]reflect.Value{intermediateIn, intermediateOut})
	}()

	go func() {
		defer close(fn.out.C)
		for {
			o, ok := intermediateOut.Recv()
			if !ok {
				return
			} else {
				fn.out.C <- o.Interface()
			}
		}
	}()
}














func Fn(fn interface{}) Transform {
	val := reflect.ValueOf(fn)
	switch val.Type().Kind() {
	case reflect.Func:
	}
	return &FnTransform{
		FnVal: reflect.ValueOf(fn),
		FnTyp: reflect.TypeOf(fn),
	}
}

type FnTransform struct {
	up    *Stream
	out   *Stream
	FnVal reflect.Value
	FnTyp reflect.Type
}

func (fn *FnTransform) input(up *Stream) {
	fn.up = up
	if fn.FnTyp.NumIn() != 1 {
		panic(fmt.Errorf("FnVal must have exactly 1 input"))
	}
	if !up.T.AssignableTo(fn.FnTyp.In(0)) {
		//TODO first look for suitable coder then panic
		panic(fmt.Errorf("cannot us Fn with input type %q to consume stream of type %q", fn.FnTyp.In(0), up.T))
	}
	if fn.FnTyp.NumOut() != 1 {
		panic(fmt.Errorf("FnVal must have exactly 1 output"))
	}
}

func (fn *FnTransform) output() *Stream {
	fn.out = &Stream{
		up: fn,
		C:  make(chan interface{}),
		T:  fn.FnTyp.Out(0),
	}
	return fn.out
}

func (fn *FnTransform) materialize() {
	go func() {
		defer close(fn.out.C)
		for d := range fn.up.Materialize() {
			v := reflect.ValueOf(d)
			r := fn.FnVal.Call([]reflect.Value{v})
			fn.out.C <- r[0].Interface()
		}
	}()
}







func RunPipeline(outputs...*Stream) {

	log.Printf("Materializing Pipeline of %d outputs\n", len(outputs))

	var channels []<-chan interface{}
	for _, s := range outputs {
		channels = append(channels, s.Materialize())
	}

	var group = new(sync.WaitGroup)
	for _, c:= range channels {
		group.Add(1)
		go func() {
			for range c {
				//TODO this is where checkpointing will hook in
			}
			group.Done()
		}()
	}

	log.Println("Draining Pipeline")

	group.Wait()

}

