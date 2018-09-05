package xmlc

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/amient/goconnect/pkg"
	"log"
	"time"
)

type XmlRecord struct {
	Position  *uint64
	Value     *Node
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
	if d.output != nil {
		panic(fmt.Errorf("xmlc.Decoder already Applied"))
	}
	d.input = source.Output()
	d.output = make(chan *XmlRecord)
	go func() {
		log.Printf("Xml Decoder Started")
		defer log.Printf("Xml Decoder Finished")
		defer close(d.output)
		for ir := range d.input {
			node, err := ReadNode(bytes.NewReader(*ir.Value))
			if err != nil {
				//TODO error porpagation instead of immediate escalation
				panic(err)
			}
			d.output <- &XmlRecord {
				Position: ir.Position,
				Timestamp: ir.Timestamp,
				Value: &node,
			}
		}
	}()
	return d
}

func (d *Decoder) Output() <-chan *XmlRecord {
	return d.output
}


func (e *Encoder) Apply(source XmlRecordSource) *Encoder {
	if e.output != nil {
		panic(fmt.Errorf("xmlc.Encoder already Applied"))
	}
	e.output = make(chan *goconnect.Record)
	e.input = source.Output()
	go func() {
		log.Printf("Xml Encoder Started")
		defer log.Printf("Xml Encoder Finished")
		defer close(e.output)
		for ir := range e.input {
			var b bytes.Buffer
			w := bufio.NewWriter(&b)
			_, err := WriteNode(w, *ir.Value)
			w.Flush()
			value := b.Bytes()
			if err != nil {
				//TODO error porpagation instead of immediate escalation
				panic(err)
			}
			e.output <- &goconnect.Record {
				Position: ir.Position,
				Timestamp: ir.Timestamp,
				Key: nil,
				Value: &value,
			}
		}
	}()
	return e
}

func (e *Encoder) Output() <-chan *goconnect.Record {
	return e.output
}
