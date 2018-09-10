package goc

import "reflect"


type RootFn interface {
	OutType() reflect.Type
	Run(output chan *Element)
}

type TransformFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Run(input <-chan *Element, output chan *Element)
}

type DoFn interface {
	InType() reflect.Type
	Run(input <-chan *Element)
}

type SideEffect interface {
	Commit(Checkpoint) error
	Close() error
}



