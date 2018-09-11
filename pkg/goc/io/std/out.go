package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"os"
	"reflect"
)

func StdOutSink() goc.ForEachFn {
	return &stdOutSink{
		stdout: bufio.NewWriter(os.Stdout),
	}
}

type stdOutSink struct {
	stdout *bufio.Writer
}

func (sink *stdOutSink) InType() reflect.Type {
	return goc.AnyType
}

func (sink *stdOutSink) Process(input *goc.Element) {
	sink.Fn(input.Value)
}

func (sink *stdOutSink) Fn(element interface{}) {
	switch e := element.(type) {
	case []byte:
		sink.stdout.Write(e)
	case string:
		sink.stdout.WriteString(e)
	case goc.KV:
		sink.Fn(e.Key)
		sink.Fn(" -> ")
		sink.Fn(e.Value)
	case goc.KVBytes:
		sink.Fn(e.Key)
		sink.Fn(" -> ")
		sink.Fn(e.Value)
	default:
		fmt.Fprint(sink.stdout, element)
	}
	sink.stdout.WriteByte('\n')
}

func (sink *stdOutSink) Flush() error {
	return sink.stdout.Flush()
}
