package goc

import "reflect"


//var _anySlice []interface{}
//var AnySliceType = reflect.TypeOf(_anySlice)
//var AnyType = AnySliceType.Elem()
//var StringType = reflect.TypeOf(string(""))



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

