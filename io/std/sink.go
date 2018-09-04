package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/io/xmlcoder"
	"log"
	"os"
)

type OutSink struct {
	input  interface{}
	output chan *goconnect.Checkpoint
	stdout *bufio.Writer
}

func (sink *OutSink) Apply(input interface{}) *OutSink {
	sink.input = input
	sink.output = make(chan *goconnect.Checkpoint)
	sink.stdout = bufio.NewWriter(os.Stdout)
	return sink
}

func (sink *OutSink) Materialize() error {
	go func() {
		log.Printf("StdOut Sink Started")
		defer log.Printf("StdOut Sink Finished")
		defer close(sink.output)
		switch t := (sink.input).(type) {
		case *xmlcoder.Decoder:
			for o := range t.Output() {
				node := *o.Value
				formatted, _ := xmlcoder.WriteNodeAsString(node)
				fmt.Fprint(sink.stdout, formatted)
				sink.output <- &goconnect.Checkpoint{
					Position: o.Position,
					Err: nil,
				}
			}
		}

	}()
	return nil
}

func (sink *OutSink) Flush() error {
	return sink.stdout.Flush()
}

func (sink *OutSink) Join() <- chan *goconnect.Checkpoint {
	return sink.output
}

func (sink *OutSink) Close() error {
	return nil
}
