package goc

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Pipeline struct {
	outputs              []*Stream
	commitInterval       time.Duration
	lastCommit           time.Time
	numProcessedElements uint32
	lastConsumedPosition interface{}
}

func RunPipeline(commitInterval time.Duration, outputs ...*Stream) error {
	p := new(Pipeline)
	p.outputs = outputs
	p.commitInterval = commitInterval
	return p.Run()
}

func (p *Pipeline) Run() error {

	log.Printf("Materializing and Running Pipeline of %d outputs\n", len(p.outputs))
	for _, stream := range p.outputs {
		stream.Materialize()
	}

	//open committer tick channel
	committerTick := time.NewTicker(p.commitInterval).C

	//open termination signal channel
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		for _, stream := range p.outputs {
			select {
			case sig := <-sigterm:
				log.Printf("Caught signal %v: terminating\n", sig)
				p.commitWorkSoFar()
				//TODO p.source.Close()

			case timestamp := <-committerTick:
				if timestamp.Sub(p.lastCommit) > p.commitInterval {
					p.commitWorkSoFar()
					p.lastCommit = timestamp
				}
			case _, more := <-stream.Channel:
				if !more {
					log.Printf("Source Channel Terminated")
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

}

func (p *Pipeline) commitWorkSoFar() {
	if p.numProcessedElements > 0 {
		log.Printf("Committing %d elements at source position: %q", p.numProcessedElements, p.lastConsumedPosition)
		for _, stream := range p.outputs {
			if err := stream.Flush(); err != nil {
				panic(err)
			}
		}
		//TODO p.source.Commit(p.lastConsumedPosition)
		log.Print("Commit successful")
		p.numProcessedElements = 0
	}

}
