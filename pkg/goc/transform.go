package goc

import "reflect"

var _anySlice []interface{}
var AnySliceType = reflect.TypeOf(_anySlice)
var AnyType = AnySliceType.Elem()




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

type ForEachDo interface {
	InType() reflect.Type
	Commit(Checkpoint) error
	Close() error
}
