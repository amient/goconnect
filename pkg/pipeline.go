package goconnect

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type pipeline struct {
	source               Source
	sink                 Sink
	commitInterval       time.Duration
	lastCommit           time.Time
	numProcessedElements uint32
	lastConsumedPosition interface {}
}

func Execute(source Source, sink Sink, commitInterval time.Duration) {
	log.Print("Materializing pipeline")

	//Materialize declared pipeline.go
	if err := source.Materialize(); err != nil {
		panic(err)
	}

	if err := sink.Materialize(); err != nil {
		panic(err)
	}

	p := &pipeline{
		source:         source,
		sink:           sink,
		commitInterval: commitInterval,
	}
	p.Run()
}

func (p *pipeline) Run() error {

	log.Print("Running pipeline")

	//open committer tick channel
	committerTick := time.NewTicker(p.commitInterval).C

	//open termination signal channel
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {

		case sig := <-sigterm:
			log.Printf("Caught signal %v: terminating\n", sig)
			p.commitWorkSoFar()
			p.source.Close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > p.commitInterval {
				p.commitWorkSoFar()
				p.lastCommit = timestamp
			}
		case checkpoint, more := <-p.sink.Join():
			if !more {
				log.Printf("Source Channel Terminated")
				p.sink.Close()
				return nil
			} else if checkpoint.Err != nil {
				panic(checkpoint.Err)
			} else {
				p.lastConsumedPosition = checkpoint.Position
				p.numProcessedElements++
			}

		}

	}
}

func (p *pipeline) commitWorkSoFar() {
	//TODO drain the pipeline first
	if p.numProcessedElements > 0 {
		log.Printf("Committing %d elements at source position: %d", p.numProcessedElements, p.lastConsumedPosition)
		if err := p.sink.Flush(); err != nil {
			panic(err)
		}
		p.source.Commit(p.lastConsumedPosition)
		log.Print("Commit successful")
		p.numProcessedElements = 0
	}

}
