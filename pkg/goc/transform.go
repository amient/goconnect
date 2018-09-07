package goc

import "reflect"

func AnyType() reflect.Type {
	var sliceOfEmptyInterface []interface{}
	return reflect.TypeOf(sliceOfEmptyInterface).Elem()
}

type Fn interface {
	Commit(Checkpoint) error
	Close() error
}

type RootTransform interface {
	Run(output chan *Element)
	OutType() reflect.Type
	Commit(Checkpoint) error
	Close() error
}

type Transform interface {
	InType() reflect.Type
	OutType() reflect.Type
	Commit(Checkpoint) error
	Close() error
}

type Do interface {
	InType() reflect.Type
	Commit(Checkpoint) error
	Close() error
}
