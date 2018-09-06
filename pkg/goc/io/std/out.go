package std

import (
	"bufio"
	"fmt"
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

func (sink *stdOutSink) Flush() error {
	return sink.stdout.Flush()
}