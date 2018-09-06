package xmlc

import (
	"bufio"
	"bytes"
	"github.com/amient/goconnect/pkg"
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"log"
	"time"
)

type XmlRecord struct {
	Position  interface{}
	Value     gocxml.Node
	Timestamp *time.Time
}

type XmlRecordSource interface {
	Output() <-chan *XmlRecord
}


type Encoder struct {
	input  <-chan *XmlRecord
	output chan *goconnect.Record
}

type Decoder struct {
	input  <-chan *goconnect.Record
	output chan *XmlRecord
}

func (d *Decoder) Apply(source goconnect.RecordSource) *Decoder {
	d.input = source.Output()
	d.output = make(chan *XmlRecord)
	go func() {
		log.Printf("Xml Decoder Started")
		defer log.Printf("Xml Decoder Finished")
		defer close(d.output)
		for ir := range d.input {
			node, err := gocxml.ReadNode(bytes.NewReader(*ir.Value))
			if err != nil {
				//TODO error porpagation instead of immediate escalation
				panic(err)
			}
			d.output <- &XmlRecord {
				Position: ir.Position,
				Timestamp: ir.Timestamp,
				Value: node,
			}
		}
	}()
	return d
}

//TODO func (e *Decoder) Materialize() error

func (d *Decoder) Output() <-chan *XmlRecord {
	return d.output
}


func (e *Encoder) Apply(source XmlRecordSource) *Encoder {
	e.input = source.Output()
	e.output = make(chan *goconnect.Record)
	go func() {
		log.Printf("Xml Encoder Started")
		defer log.Printf("Xml Encoder Finished")
		defer close(e.output)
		for inputRecord := range e.input {
			var b bytes.Buffer
			w := bufio.NewWriter(&b)
			_, err := gocxml.WriteNode(w, inputRecord.Value)
			w.Flush()
			value := b.Bytes()
			if err != nil {
				//TODO error porpagation instead of immediate escalation
				panic(err)
			}
			e.output <- &goconnect.Record {
				Position:  inputRecord.Position,
				Timestamp: inputRecord.Timestamp,
				Key:       nil,
				Value:     &value,
			}
		}
	}()
	return e
}

//TODO func (e *Encoder) Materialize() error

func (e *Encoder) Output() <-chan *goconnect.Record {
	return e.output
}
