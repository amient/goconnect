package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"os"
)

func StdOutSink() *stdOutSink {
	return &stdOutSink {
		stdout: bufio.NewWriter(os.Stdout),
	}
}

type stdOutSink struct {
	stdout   *bufio.Writer
}

func (sink *stdOutSink) Fn(element interface {}) error {
	fmt.Fprint(sink.stdout, element)
	return nil
}

func (sink *stdOutSink) Commit(goc.Checkpoint) error {
	return sink.stdout.Flush()
}