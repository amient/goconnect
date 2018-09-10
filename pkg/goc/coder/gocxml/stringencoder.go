package gocxml

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

