package io

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func Iterable(list interface{}) *goc.Stream {
	val := reflect.ValueOf(list)
	return &goc.Stream{
		Type: reflect.TypeOf(list).Elem(),
		Materializer: func(output chan interface{}) {
			for i := 0; i < val.Len(); i++ {
				output <- val.Index(i).Interface()
			}
		},
	}

}
