package gocxml

import (
	"bytes"
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func BytesDecoder() goc.MapFn {
	return &bytesDecoder{}
}

type bytesDecoder struct{}

func (d *bytesDecoder) InType() reflect.Type {
	return goc.ByteArrayType
}

func (d *bytesDecoder) OutType() reflect.Type {
	return NodeType
}

func (d *bytesDecoder) Process(input *goc.Element) *goc.Element {
	if node, err := ReadNode(bytes.NewReader(input.Value.([]byte))); err != nil {
		panic(err)
	} else {
		return &goc.Element{Value: node}
	}

}
