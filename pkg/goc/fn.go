package goc

import "reflect"

type Fn interface {}

type RootFn interface {
	OutType() reflect.Type
	Run(output OutputChannel)
}

type MapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) *Element
}

type ForEachFn interface {
	InType() reflect.Type
	Process(input *Element)
}

type FlatMapFn interface {
	InType() reflect.Type
	OutType() reflect.Type
	Process(input *Element) []*Element
}

//type TransformFn interface {
//	InType() reflect.Type
//	OutType() reflect.Type
//	Run(input InputChannel, output OutputChannel)
//}

type Closeable interface {
	Close() error
}

type SideEffect interface {
	Flush() error
}

type Commitable interface {
	Commit(Checkpoint) error
}



