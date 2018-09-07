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

func (sink *stdOutSink) Fn(element interface {}) error {
	switch e := element.(type) {
		case []byte: sink.stdout.Write(e)
		case string: sink.stdout.WriteString(e)
		default: fmt.Fprint(sink.stdout, element)
	}
	sink.stdout.WriteByte('\n')
	sink.stdout.Flush()
	return nil
}

func (sink *stdOutSink) Commit(goc.Checkpoint) error {
	return sink.stdout.Flush()
}

func (sink *stdOutSink) Close() error {
	return nil
}