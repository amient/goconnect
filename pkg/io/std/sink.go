package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg"
	"log"
	"os"
)

type OutSink struct {
	upstream goconnect.RecordSource
	output   chan *goconnect.Checkpoint
	stdout   *bufio.Writer
}

func (sink *OutSink) Apply(upstream goconnect.RecordSource) *OutSink {
	sink.upstream = upstream
	if sink.output != nil {
		panic(fmt.Errorf("OutSink was already applied"))
	}
	sink.output = make(chan *goconnect.Checkpoint)
	sink.stdout = bufio.NewWriter(os.Stdout)
	return sink
}

func (sink *OutSink) Materialize() error {
	go func() {
		log.Printf("StdOut Sink Started")
		defer log.Printf("StdOut Sink Finished")
		defer close(sink.output)
		for o := range sink.upstream.Output() {
			bytes := *o.Value
			if len(bytes) > 0 {
				sink.stdout.Write(bytes)
			}
			sink.output <- goconnect.NewCheckpoint(o.Position)
		}
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
