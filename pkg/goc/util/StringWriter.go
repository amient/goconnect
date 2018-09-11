package util

import (
	"strings"
)

type StringWriter struct {
	builder strings.Builder
}

func NewStringWriter() *StringWriter {
	return &StringWriter{
		builder: *new(strings.Builder),
	}
}

func (w *StringWriter)  Write(bytes []byte) (int, error) {
	return w.builder.Write(bytes)
}

func (w *StringWriter) String() string {
	return w.builder.String()
}