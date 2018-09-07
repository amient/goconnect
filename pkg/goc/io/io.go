package io

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func Iterable(list interface{}) goc.RootTransform {
	//TODO validate array or slice
	return &iterable{
		val: reflect.ValueOf(list),
		typ: reflect.TypeOf(list),
	}
}

type iterable struct {
	val reflect.Value
	typ reflect.Type
}

func (it *iterable) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *iterable) Run(output chan *goc.Element) {
	//OutType:
	for i := 0; i < it.val.Len(); i++ {
		output <- &goc.Element{
			Checkpoint: i,
			Value:      it.val.Index(i).Interface(),
		}
	}
}


func (it *iterable) Commit(goc.Checkpoint) error {
	return nil
}

func (it *iterable) Close() error {
	return nil
}
