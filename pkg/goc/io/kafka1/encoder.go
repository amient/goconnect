package kafka1

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func NilKeyEncoder() *encoder {
	return &encoder{}
}

type encoder struct{}

func (d *encoder) InType() reflect.Type {
	return goc.ByteArrayType
}

func (d *encoder) OutType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (d *encoder) Process(input *goc.Element) *goc.Element {
	return &goc.Element{Value: goc.KVBytes{
		Key: nil,
		Value: input.Value.([]byte),
	}}
}

