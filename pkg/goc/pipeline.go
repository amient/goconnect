package goc

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"
)

type Pipeline struct {
	Control              *Control
	streams              []*Stream
	source               RootTransform
	lastCommit           time.Time
	numProcessedElements uint32
	lastConsumedPosition interface{}
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		streams: []*Stream{},
	}
}

func (p *Pipeline) From(source RootTransform) *Stream {
	return p.Register(&Stream{
		Type:      source.OutType(),
		runner:    source.Run,
		transform: source,
	})
}

func (p *Pipeline) Transform(that *Stream, out reflect.Type, fn func(input chan *Element, output chan *Element)) *Stream {
	return p.Register(&Stream{
		Type: out,
		runner: func(output chan *Element) {
			transient := make(chan *Element)
			go func(in *Stream) {
				defer close(transient)
				for d := range in.output {
					if d.isMarker() {
						output <- d
					} else {
						transient <- d
					}
				}
			}(that)
			fn(transient, output)
		},
	})

}


func (p *Pipeline) Register(stream *Stream) *Stream {
	stream.pipeline = p
	p.streams = append(p.streams, stream)
	return stream
}

func (p *Pipeline) Run(commitInterval time.Duration) error {

	log.Printf("Materializing and Running Pipeline of %d stages\n", len(p.streams))
	p.Control = &Control{make(chan *ControlSignal)}

	for _, stream := range p.streams {
		log.Printf("Materilaizing Stream of %q \n", stream.Type)
		if stream.output != nil {
			panic(fmt.Errorf("stream already materialized"))
		}

		interceptedOutput := make(chan *Element)
		go func(stream *Stream) {
			defer close(interceptedOutput)
			stream.runner(interceptedOutput)
		}(stream)

		stream.output = make(chan *Element)
		go func(stream *Stream) {
			defer close(stream.output)
			for {
				select {
				case <-p.Control.Signals(stream.output):
				case element, ok := <-interceptedOutput:
					if !ok {
						return
					} else {
						stream.output <- element
					}
				}
			}
		}(stream)
	}

	p.source = p.streams[0].transform.(RootTransform)
	sink := p.streams[len(p.streams)-1]

	//open committer tick underlying
	committerTick := time.NewTicker(commitInterval).C

	//open termination signal underlying
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigterm:
			log.Printf("Caught signal %v: terminating\n", sig)
			p.Control.Send(ControlCleanStop)

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > commitInterval {
				p.startCommit(timestamp)
			}

		case e, more := <-sink.output:
			if !more {
				//this is the only place that exits the for-select
				p.concludeCommit()
				p.Control.Close()
				//TODO p.sink.Close()
				return nil
			} else if e.isMarker() {
				p.concludeCommit()
				p.Control.Send(ControlResume)
			} else {
				//TODO p.lastConsumedPosition = checkpoint.Position
				p.numProcessedElements++
			}

		}

	}

}
func (p *Pipeline) startCommit(timestamp time.Time) {
	if p.numProcessedElements > 0 {
		log.Println("Start Commit")
		//draining should not be necessary if optimistic checkpointing is possible - see comments in goc.Checkpoint
		p.Control.Send(ControlDrain)
		p.lastCommit = timestamp
	}

}

func (p *Pipeline) concludeCommit() {
	if p.numProcessedElements > 0 {
		log.Printf("Committing %d elements at source position: %q", p.numProcessedElements, p.lastConsumedPosition)

		for i := len(p.streams) - 1; i >= 0; i-- {
			stream := p.streams[i]
			if stream.transform != nil {
				//TODO collect emited stream.transform.checkpoint associated with output.checkkpoint
				if err := stream.transform.Commit(nil); err != nil {
					panic(err)
				}
			}
		}
		log.Print("Commit successful")
		p.numProcessedElements = 0
	}

}