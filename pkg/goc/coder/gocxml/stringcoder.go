package gocxml

func StringDecoder() *stringDecoder {
	return &stringDecoder{}
}


type stringDecoder struct {}

func (d *stringDecoder) Fn(input string) Node {
	var node, err = ReadNodeFromString(input)
	if err != nil {
		panic(err)
	}
	return node
}

func (d *stringDecoder) Flush() error {
	return nil
}


func StringEncoder() *stringEncoder {
	return &stringEncoder{}
}

type stringEncoder struct {}

func (e *stringEncoder) Fn(input Node) string {
	s, err := WriteNodeAsString(input)
	if err != nil {
		panic(err)
	}
	return s
}

func (e *stringEncoder) Flush() error {
	return nil
}


