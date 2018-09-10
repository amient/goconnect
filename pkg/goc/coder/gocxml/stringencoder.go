package gocxml

import "github.com/amient/goconnect/pkg/goc"

func StringEncoder() *stringEncoder {
	return &stringEncoder{}
}

type stringEncoder struct{}

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

