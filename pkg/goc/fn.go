package goc

import "reflect"


type Fn struct {
	in    *Stream
	out   *Stream
	FnVal reflect.Value
	FnTyp reflect.Type
	mat   func()
}

func (fn *Fn) materialize() {
	go func() {
		defer close(fn.out.Channel)
		fn.mat()
	}()
}

