package goc

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Pipeline struct {
	streams				 []*Stream
	lastCommit           time.Time
	numProcessedElements uint32
	lastConsumedPosition interface{}
}

func NewPipeline() *Pipeline {
	return &Pipeline {
		streams: []*Stream{},
	}
}

func (p *Pipeline) Run(commitInterval time.Duration) error {

	log.Printf("Materializing and Running Pipeline of %d stages\n", len(p.streams))
	for _, stream := range p.streams {
		stream.Materialize()
	}
	output := p.streams[len(p.streams) -1]

	//open committer tick underlying
	committerTick := time.NewTicker(commitInterval).C

	//open termination signal underlying
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigterm:
			log.Printf("Caught signal %v: terminating\n", sig)
			p.commitWorkSoFar()
			//TODO p.source.Close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > commitInterval {
				p.commitWorkSoFar()
				p.lastCommit = timestamp
			}
		case _, more := <- output.underlying:
			if !more {
				p.commitWorkSoFar()
				//TODO p.sink.Close()
				return nil
			} else {
				//TODO p.lastConsumedPosition = checkpoint.Position
				p.numProcessedElements++
			}

		}

	}

}

func (p *Pipeline) commitWorkSoFar() {
	if p.numProcessedElements > 0 {
		log.Printf("Committing %d elements at source position: %q", p.numProcessedElements, p.lastConsumedPosition)
		for _, stream := range p.streams {
			if err := stream.Commit(Checkpoint{}); err != nil {
				panic(err)
			}
		}
		log.Print("Commit successful")
		p.numProcessedElements = 0
	}

}

func (p *Pipeline) From(stream *Stream) *Stream {
	stream.pipeline = p
	p.streams = append(p.streams, stream)
	return stream
}
