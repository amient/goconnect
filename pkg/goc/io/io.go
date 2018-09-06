package io

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
)

func FromList(list interface{}) goc.Stream {
	val := reflect.ValueOf(list)
	result := goc.Stream {
		Channel: make(chan interface{}),
		Type:    reflect.TypeOf(list).Elem(),
	}
	go func() {
		for i := 0; i < val.Len(); i++ {
			result.Channel <- val.Index(i).Interface()
		}
		log.Println("Closing Root List")
		close(result.Channel)

	}()
	return result
}
