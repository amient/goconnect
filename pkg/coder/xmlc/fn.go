package xmlc


func Modify(fn func(Node) (Node, error)) *XmlFilterModifyFn {
	return &XmlFilterModifyFn{fn: fn}
}

func Filter(fn func(Node) (bool, error)) *XmlFilterModifyFn {
	return &XmlFilterModifyFn{fn: func(n Node) (Node, error) {
		if pass, err := fn(n); pass {
			return n, err
		} else {
			return nil, err
		}
	}}
}


type XmlFilterModifyFn struct {
	fn     func(Node) (Node, error)
	input  <-chan *XmlRecord
	output chan *XmlRecord
}

func (m *XmlFilterModifyFn) Output() <-chan *XmlRecord {
	return m.output
}

func (m *XmlFilterModifyFn) Apply(upstream XmlRecordSource) *XmlFilterModifyFn {
	m.input = upstream.Output()
	m.output = make(chan *XmlRecord)
	go func() {
		defer close(m.output)
		for inputRecord := range m.input {
			node, err := m.fn(inputRecord.Value)
			if err != nil {
				//TODO error porpagation instead of immediate escalation
				panic(err)
			}
			if node != nil {
				m.output <- &XmlRecord{
					Value: node,
					Position: inputRecord.Position,
					Timestamp: inputRecord.Timestamp,
				}
			}
		}
	}()
	return m
}

