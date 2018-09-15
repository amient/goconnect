package io

import (
	"github.com/amient/goconnect/pkg/goc"
	"math/rand"
	"reflect"
)

func RandomOf(n int, list interface{}) goc.RootFn {
	//TODO validate array or slice
	return &randomOf{
		n: n,
		val: reflect.ValueOf(list),
		typ: reflect.TypeOf(list),
	}
}

type randomOf struct {
	n   int
	val reflect.Value
	typ reflect.Type
}

func (it *randomOf) OutType() reflect.Type {
	return it.typ.Elem()
}

func (it *randomOf) Run(output goc.OutputChannel) {
	size := it.val.Len()
	for l := 0; l < it.n; l++ {
		i := rand.Int() % size
		output <- &goc.Element{
			Checkpoint: goc.Checkpoint{Data: i},
			Value:      it.val.Index(i).Interface(),
		}
	}
}

func (it *randomOf) Commit(checkpoint map[int]interface{}) error {
	//log.Println("ACK-UP-TO", checkpoint[0])
	return nil
}
