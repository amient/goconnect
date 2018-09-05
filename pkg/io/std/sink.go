package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg"
	"github.com/amient/goconnect/pkg/coder/xmlc"
	"log"
	"os"
	"reflect"
)

type OutSink struct {
	upstream interface{}
	output   chan *goconnect.Checkpoint
	stdout   *bufio.Writer
}

func (sink *OutSink) Apply(upstream interface{}) *OutSink {
	sink.upstream = upstream
	if sink.output != nil {
		panic(fmt.Errorf("OutSink was already applied"))
	}
	sink.output = make(chan *goconnect.Checkpoint)
	sink.stdout = bufio.NewWriter(os.Stdout)
	return sink
}

func (sink *OutSink) Materialize() error {
	var processor func()
	if u, is := sink.upstream.(goconnect.RecordSource); is {
		log.Print("StdOut Sink consuming byte record stream")
		processor = func() {
			for o := range u.Output() {
				bytes := *o.Value
				if len(bytes) > 0 {
					sink.stdout.Write(bytes)
				}
				sink.output <- goconnect.NewCheckpoint(o.Position)
			}
		}
	} else if u, is := sink.upstream.(xmlc.XmlRecordSource); is {
		log.Print("StdOut Sink consuming xml record stream")
		processor = func() {
			for o := range u.Output() {
				node := *o.Value
				formatted, _ := xmlc.WriteNodeAsString(node)
				fmt.Fprint(sink.stdout, formatted)
				sink.output <- goconnect.NewCheckpoint(o.Position)
			}
		}
	} else {
		return fmt.Errorf("Unsupported type ", reflect.TypeOf(sink.upstream))
	}

	go func() {
		log.Printf("StdOut Sink Started")
		defer log.Printf("StdOut Sink Finished")
		defer close(sink.output)
		processor()
	}()

	return nil
}

func (sink *OutSink) Flush() error {
	return sink.stdout.Flush()
}

func (sink *OutSink) Join() <-chan *goconnect.Checkpoint {
	return sink.output
}

func (sink *OutSink) Close() error {
	return nil
}
