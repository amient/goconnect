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
	streams    []*Stream
	lastCommit time.Time
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		streams: []*Stream{},
	}
}

func (p *Pipeline) register(stream *Stream) *Stream {
	stream.pipeline = p
	p.streams = append(p.streams, stream)
	return stream
}

func (p *Pipeline) Root(source RootFn) *Stream {
	return p.register(&Stream{
		Type:      source.OutType(),
		runner:    source.Run,
		transform: source,
	})
}

func (p *Pipeline) Apply(that *Stream, out reflect.Type, fn Fn, run func(input <-chan *Element, output chan *Element)) *Stream {
	return p.register(&Stream{
		Type:      out,
		transform: fn,
		runner: func(output chan *Element) {
			run(that.forward(output), output)
		},
	})
}

func (p *Pipeline) Do(that *Stream, fn DoFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.Apply(that, ErrorType, fn, func(input <-chan *Element, output chan *Element) {
		fn.Run(that.forward(output))
	})
}

func (p *Pipeline) Transform(that *Stream, fn TransformFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.Apply(that, fn.OutType(), fn, fn.Run)
}

func (p *Pipeline) Run(commitInterval time.Duration) {

	log.Printf("Running Pipeline of %d stages\n", len(p.streams))

	for s, stream := range p.streams {
		log.Printf("Materilaizing Stream of %q \n", stream.Type)
		if stream.output != nil {
			panic(fmt.Errorf("stream already materialized"))
		}

		stream.output = make(chan *Element)
		go func(s int, stream *Stream) {
			defer log.Println("stage terminated[%d]: %q", s, stream.Type)
			defer stream.close()
			stream.runner(stream.output)
		}(s, stream)

	}

	sink := p.streams[len(p.streams)-1]

	//open committer tick underlying
	committerTick := time.NewTicker(commitInterval).C

	//open termination signal underlying
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigterm:
			log.Printf("Caught signal %v: Cancelling\n", sig)
			//TODO assuming a single root
			p.streams[0].close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > commitInterval {
				log.Println("Start Commit")
				//draining should not be necessary if optimistic checkpointing is possible - see comments in goc.Checkpoint
				p.streams[0].output <- &Element{signal: ControlDrain}
				log.Println("Control Drain")
				p.lastCommit = timestamp
			}

		case e, more := <-sink.output:
			if !more {
				log.Println("Pipeline Terminated")
				p.commit() //TODO this commit is here for bounded pipelines but needs to be verified that it doesn't break guarantees of unbouded pipeliens
				for i := len(p.streams) - 1; i >= 0; i-- {
					stream := p.streams[i]
					if stream.transform != nil {
						stream.transform.Close()
					}
				}
				//this is the only place that exits the for-select which in turn is only possible by ControlCancel signal below
				return
			} else if e.hasSignal() {
				switch e.signal {

				case ControlDrain:
					log.Printf("Concluding Commit via Marker")
					p.commit()

				//case ControlCancel:
				//	p.Control.Close()
				//	for i := len(p.streams) - 1; i >= 0; i-- {
				//		stream := p.streams[i]
				//		if stream.transform != nil {
				//			stream.transform.Close()
				//		}
				//	}
				}
			}

		}

	}
}

func (p *Pipeline) commit() {
	for i := len(p.streams) - 1; i >= 0; i-- {
		stream := p.streams[i]
		if stream.transform != nil {
			//TODO collect emited stream.transform.checkpoint associated with output.checkkpoint
			if err := stream.transform.Commit(nil); err != nil {
				panic(err)
			}
		}
	}
}
