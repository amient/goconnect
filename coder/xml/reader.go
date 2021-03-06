package xml

import (
	encXml "encoding/xml"
	"io"
	"strings"
)

func ReadNodeFromString(input string) (Node, error) {
	return ReadNode(strings.NewReader(input))
}

// Reads all XML data from the given reader and stores it in a root node.
func ReadNode(r io.Reader) (Node, error) {
	// Create root node.
	// Starting with Tag instead of Root, to eliminate type checks when refering
	// to parent nodes during reading. Will be replaced with a Root node at the
	// end.
	result := &tag{
		nil,
		"",
		nil,
		nil,
	}
	dec := encXml.NewDecoder(r)

	var t encXml.Token
	var err error
	var current *tag
	current = result

	// Parse tokens.
	for t, err = dec.Token(); err == nil; t, err = dec.Token() {
		switch t := t.(type) {
		case encXml.StartElement:
			// Copy attributes.
			attrs := make([]*encXml.Attr, len(t.Attr))
			for i, attr := range t.Attr {
				attrs[i] = &encXml.Attr{attr.Name, attr.Value}
			}

			// Create child node.
			child := &tag{
				current,
				t.Name.Local,
				attrs,
				nil,
			}

			current.children = append(current.children, child)
			current = child

		case encXml.EndElement:
			current = current.Parent().(*tag)

		case encXml.CharData:
			child := &text{
				current,
				string(t),
			}
			current.children = append(current.children, child)

		case encXml.Comment:
			child := &comment{
				current,
				string(t),
			}

			current.children = append(current.children, child)

		case encXml.ProcInst:
			child := &procInst{
				current,
				string(t.Target),
				string(t.Inst),
			}

			current.children = append(current.children, child)

		case encXml.Directive:
			child := &directive{
				current,
				string(t),
			}

			current.children = append(current.children, child)
		}

	}

	// EOF is ok.
	if err != io.EOF {
		return nil, err
	}

	return &root{result.children}, nil
}