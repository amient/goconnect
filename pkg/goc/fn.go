package goc

import "reflect"


type Fn interface {
	Commit(Checkpoint) error
	Close() error
}

type RootFn interface {
	OutType() reflect.Type
	Run(output chan *Element)
	Commit(Checkpoint) error
	Close() error
}

type TransformFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Run(input <-chan *Element, output chan *Element)
	Commit(Checkpoint) error
	Close() error
}

type DoFn interface {
	InType() reflect.Type
	Run(input <-chan *Element)
	Commit(Checkpoint) error
	Close() error
}


