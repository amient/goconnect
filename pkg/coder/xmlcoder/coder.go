package xmlcoder

import (
	"bytes"
	"github.com/amient/goconnect/pkg"
	"log"
	"time"
)

type XmlRecord struct {
	Position  *uint64
	Value     *Node
	Timestamp *time.Time
}

type XmlStream <- chan *XmlRecord

type Decoder struct {
	errc   chan error
	input  goconnect.RecordStream
	output chan *XmlRecord
}

func (d *Decoder) Apply(source goconnect.RecordSource) goconnect.Decoder {
	d.errc = make(chan error, 1)
	d.input = source.Apply()
	d.output = make(chan *XmlRecord)
	go func() {
		log.Printf("Xml Decoder Started")
		defer log.Printf("Xml Decoder Finished")
		defer close(d.output)
		for ir := range d.input {
			node, err := ReadNode(bytes.NewReader(*ir.Value))
			if err != nil {
				//TODO propage the errors
				panic(err)
			}
			d.output <- &XmlRecord {
				Position: ir.Position,
				Timestamp: ir.Timestamp,
				Value: &node,
			}
		}
		d.errc <- nil
	}()
	return d
}

func (d *Decoder) Output() XmlStream {
	return d.output
}


func (d *Decoder) Close() error {
	return nil
}
