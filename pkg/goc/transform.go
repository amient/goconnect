package goc

import "reflect"


type Fn interface {
	Commit(Checkpoint) error
	Close() error
}

type RootFn interface {
	Run(output chan *Element)
	OutType() reflect.Type
	Commit(Checkpoint) error
	Close() error
}

