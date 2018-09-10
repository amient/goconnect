package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"os"
	"reflect"
)

func StdOutSink() *stdOutSink {
	return &stdOutSink {
		stdout: bufio.NewWriter(os.Stdout),
	}
}

type stdOutSink struct {
	stdout   *bufio.Writer
}

func (sink *stdOutSink) InType() reflect.Type {
	return goc.AnyType
}

func (sink *stdOutSink) Run(input <- chan *goc.Element) {
	for e := range input {
		sink.Fn(e.Value)
	}
}

func (sink *stdOutSink) Fn(element interface {}) error {
	switch e := element.(type) {
		case []byte: sink.stdout.Write(e)
		case string: sink.stdout.WriteString(e)
		case goc.KV:
			sink.Fn(e.Key)
			sink.Fn(" -> ")
			sink.Fn(e.Value)
		case goc.KVBytes:
			sink.Fn(e.Key)
			sink.Fn(" -> ")
			sink.Fn(e.Value)
		default: fmt.Fprint(sink.stdout, element)
	}
	sink.stdout.WriteByte('\n')
	return nil
}

func (sink *stdOutSink) Flush(*goc.Checkpoint) error {
	return sink.stdout.Flush()
}
