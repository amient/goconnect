package goconnect

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Pipeline struct {
	source               *Source
	sink                 *Sink
	commitInterval       *time.Duration
	lastCommit           time.Time
	copiedMessages       uint32
	copiedBytes          uint64
	lastConsumedPosition uint64
}

func Execute(source Source, sink Sink, commitInterval time.Duration) {
	log.Print("Materializing Pipeline")

	//Materialize declared pipeline.go
	if err := source.Materialize(); err != nil {
		panic(err)
	}

	if err := sink.Materialize(); err != nil {
		panic(err)
	}

	p := &Pipeline{
		source:         &source,
		sink:           &sink,
		commitInterval: &commitInterval,
	}
	p.Run()
}

func (p *Pipeline) Run() error {

	log.Print("Running pipeline")

	//open committer tick channel
	committerTick := time.NewTicker(*p.commitInterval).C

	//open termination signal channel
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {

		case sig := <-sigterm:
			log.Printf("Caught signal %v: terminating\n", sig)
			p.commitWorkSoFar()
			(*p.source).Close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > *p.commitInterval {
				p.commitWorkSoFar()
				p.lastCommit = timestamp
			}
		case checkpoint, more := <-(*p.sink).Join():
			if !more {
				log.Printf("Source Channel Terminated")
				(*p.sink).Close()
				return nil
			} else if checkpoint.Err != nil {
				panic(checkpoint.Err)
			} else {
				p.lastConsumedPosition = *checkpoint.Position
				p.copiedMessages++
				//p.copiedBytes += uint64(len(*msg.Value)) //FIXME
			}

		}

	}
}

func (p *Pipeline) commitWorkSoFar() {
	if p.copiedMessages > 0 {
		log.Printf("Committing %d messages / %d bytes", p.copiedMessages, p.copiedBytes)
		if err := (*p.sink).Flush(); err != nil {
			panic(err)
		}
		(*p.source).Commit(p.lastConsumedPosition)
		log.Print("Commit successful")
		p.copiedMessages = 0
		p.copiedBytes = 0
	}

}
