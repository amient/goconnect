package goc

import "reflect"

type Fn interface {}

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

type Closeable interface {
	Close() error
}

type SideEffect interface {
	Flush(*Checkpoint) error
}

type Commitable interface {
	Commit(*Checkpoint) error
}



