package gocxml

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func StringDecoder() *stringDecoder {
	return &stringDecoder{}
}

type stringDecoder struct{}

func (d *stringDecoder) InType() reflect.Type {
	return goc.StringType
}

func (d *stringDecoder) OutType() reflect.Type {
	return reflect.TypeOf([]Node{}).Elem()
}

func (d *stringDecoder) Process(input *goc.Element) *goc.Element {
	return &goc.Element{Value: d.Fn(input.Value.(string))}
}

func (d *stringDecoder) Fn(input string) Node {
	var node, err = ReadNodeFromString(input)
	if err != nil {
		panic(err)
	} else {
		return node
	}

}

