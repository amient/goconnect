package gocxml

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func StringDecoder() *stringDecoder {
	return &stringDecoder{}
}


type stringDecoder struct {}

func (d *stringDecoder) InType() reflect.Type {
	return goc.StringType
}

func (d *stringDecoder) Fn(input string) Node {
	var node, err = ReadNodeFromString(input)
	if err != nil {
		panic(err)
	}
	return node
}

func (d *stringDecoder) Commit(checkpoint goc.Checkpoint) error {
	return nil
}

func (d*stringDecoder) Close() error {
	return nil
}



func StringEncoder() *stringEncoder {
	return &stringEncoder{}
}

type stringEncoder struct {}

func (e *stringEncoder) Fn(input Node) string {
	s, err := WriteNodeAsString(input)
	if err != nil {
		panic(err)
	}
	return s
}

func (e *stringEncoder) Commit(checkpoint goc.Checkpoint) error {
	return nil
}

func (e *stringEncoder) Close() error {
	return nil
}
