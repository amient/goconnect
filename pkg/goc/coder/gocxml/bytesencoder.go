package gocxml

import (
	"bufio"
	"bytes"
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func BytesEncoder() goc.MapFn {
	return &bytesEncoder{}
}

type bytesEncoder struct{}

func (d *bytesEncoder) InType() reflect.Type {
	return NodeType
}

func (d *bytesEncoder) OutType() reflect.Type {
	return goc.StringType
}

func (d *bytesEncoder) Process(input *goc.Element) *goc.Element {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	_, err := WriteNode(w, input.Value.(Node))
	w.Flush()
	if err != nil {
		panic(err)
	} else {
		return &goc.Element{Value: b.Bytes()}
	}
}
