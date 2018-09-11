package gocxml

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func StringDecoder() goc.MapFn {
	return &stringDecoder{}
}

type stringDecoder struct{}

func (d *stringDecoder) InType() reflect.Type {
	return goc.StringType
}

func (d *stringDecoder) OutType() reflect.Type {
	return NodeType
}

func (d *stringDecoder) Process(input *goc.Element) *goc.Element {
	if node, err := ReadNodeFromString(input.Value.(string)); err != nil {
		panic(err)
	} else {
		return &goc.Element{Value: node}
	}

}
