package gocxml

import (
	"bufio"
	"bytes"
	"github.com/amient/goconnect/pkg/goc"
)

func BytesDecoder() *bytesDecoder {
	return &bytesDecoder{}
}
type bytesDecoder struct {}

func (d *bytesDecoder) Fn(input []byte) Node {
	var node, err = ReadNode(bytes.NewReader(input))
	if err != nil {
		panic(err)
	}
	return node
}

func (d *bytesDecoder) Commit(goc.Checkpoint) error {
	return nil
}


func BytesEncoder() *bytesEncoder {
	return &bytesEncoder{}
}

type bytesEncoder struct {}

func (e *bytesEncoder) Fn(input Node) []byte {
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	_, err := WriteNode(w, input)
	w.Flush()
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

func (e *bytesEncoder) Commit(goc.Checkpoint) error {
	return nil
}
