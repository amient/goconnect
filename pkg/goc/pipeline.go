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
		Type:   source.OutType(),
		fn: source,
		runner: source.Run,
	})
}

func (p *Pipeline) FlatMap(that *Stream, fn FlatMapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		for i, outputElement := range fn.Process(input) {
			if outputElement.Timestamp == nil {
				outputElement.Timestamp = input.Timestamp
			}
			if outputElement.Checkpoint == nil {
				outputElement.Checkpoint = Checkpoint{0: i}
			}
			output <- outputElement
		}
	})
}

func (p *Pipeline) Map(that *Stream, fn MapFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, fn.OutType(), fn, func(input *Element, output OutputChannel) {
		outputElement := fn.Process(input)
		if outputElement.Timestamp == nil {
			outputElement.Timestamp = input.Timestamp
		}
		output <- outputElement
	})
}

func (p *Pipeline) ForEach(that *Stream, fn ForEachFn) *Stream {
	if !that.Type.AssignableTo(fn.InType()) {
		//TODO this check will be removed and resolved during coder injection step
		panic(fmt.Errorf("cannot Apply Fn with input type %q to consume stream of type %q",
			fn.InType(), that.Type))
	}
	return p.elementWise(that, ErrorType, fn, func(input *Element, output OutputChannel) {
		fn.Process(input)
	})
}


func (p *Pipeline) elementWise(that *Stream, out reflect.Type, fn interface{}, run func(input *Element, output OutputChannel)) *Stream {
	return p.register(&Stream{
		Type: out,
		fn: fn,
		runner: func(output OutputChannel) {
			var checkpoint = make(Checkpoint)
			for element := range that.output {
				checkpoint.merge(element.Checkpoint)
				switch element.signal {
				case NoSignal:
					run(element, output)
				case ControlDrain:
					output <- element
					that.checkpoint.merge(checkpoint)
					checkpoint = make(Checkpoint)
					if fn, ok := that.fn.(SideEffect); ok {
						fn.Flush()
					}
				}
			}
		},
	})
}


func (p *Pipeline) Run(commitInterval time.Duration) {

	log.Printf("Running Pipeline of %d stages\n", len(p.streams))

	for s, stream := range p.streams {
		log.Printf("Materilaizing Stream of %q \n", stream.Type)
		if stream.output != nil {
			panic(fmt.Errorf("stream already materialized"))
		}

		stream.checkpoint = make(Checkpoint)
		stream.output = make(chan *Element)
		go func(s int, stream *Stream) {
			defer log.Printf("Stage terminated[%d]: %q\n", s, stream.Type)
			defer stream.close()
			stream.runner(stream.output)
			if !stream.closed {
				//this is here to terminate bounded sources with a commit
				stream.output <- &Element{signal: ControlDrain}
			}
		}(s, stream)

	}

	source := p.streams[0]
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
			source.close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > commitInterval {
				//log.Println("Start Commit")
				if !source.closed {
					source.output <- &Element{signal: ControlDrain}
				}
				p.lastCommit = timestamp
			}

		case e, more := <-sink.output:
			if !more {
				log.Println("Pipeline Terminated")
				p.commit() //TODO this commit is here for bounded pipelines but needs to be verified that it doesn't break guarantees of unbouded pipeliens
				for i := len(p.streams) - 1; i >= 0; i-- {
					stream := p.streams[i]
					if fn, ok := stream.fn.(Closeable); ok {
						if err := fn.Close(); err != nil {
							panic(err)
						}
					}
				}
				//this is the only place that exits the for-select which in turn is only possible by ControlCancel signal below
				return
			}
			switch e.signal {
			case NoSignal:
			case ControlDrain:
				//FIXME
				if fn, ok := sink.fn.(SideEffect); ok {
					fn.Flush()
				}
				p.commit()

			}


		}

	}
}

func (p *Pipeline) commit() {
	for i := len(p.streams) - 1; i >= 0; i-- {
		if p.streams[i].commit() {
			//log.Printf("Committed stage %d\n", i)
		}
	}
	//log.Printf("Committing completed\n")
}
