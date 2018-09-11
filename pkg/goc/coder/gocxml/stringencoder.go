package gocxml

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func StringEncoder() goc.MapFn {
	return &stringEncoder{}
}

type stringEncoder struct{}

func (d *stringEncoder) InType() reflect.Type {
	return NodeType
}

func (d *stringEncoder) OutType() reflect.Type {
	return goc.StringType
}

func (d *stringEncoder) Process(input *goc.Element) *goc.Element {
	str, err := WriteNodeAsString(input.Value.(Node))
	if err != nil {
		panic(err)
	} else {
		return &goc.Element{Value: str}
	}
}
