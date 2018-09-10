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

func (d *stringDecoder) Run(input <-chan *goc.Element, output chan *goc.Element) {
	for in := range input {
		output <- &goc.Element{Value: d.Fn(in.Value.(string))}
	}
}

func (d *stringDecoder) Fn(input string) Node {
	var node, err = ReadNodeFromString(input)
	if err != nil {
		panic(err)
	} else {
		return node
	}

}

func (d *stringDecoder) Commit(checkpoint goc.Checkpoint) error {
	return nil
}

func (d *stringDecoder) Close() error {
	return nil
}

