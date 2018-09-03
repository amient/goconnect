package goconnect

import (
	"fmt"
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

func CreatePipeline(source *Source, sink *Sink, commitInterval *time.Duration) (*Pipeline) {
	//initialize declared pipeline.go
	if err := (*source).Initialize(); err != nil {
		panic(err)
	}

	if err := (*sink).Initialize(); err != nil {
		panic(err)
	}
	return &Pipeline{
		source:         source,
		sink:           sink,
		commitInterval: commitInterval,
	}
}

func (p *Pipeline) Run() {

	//open input data stream channel
	input := (*p.source).Records()

	//open committer tick channel
	committerTick := time.NewTicker(*p.commitInterval).C

	//open termination signal channel
	sigterm := make(chan error)
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGSTOP)
		sigterm <- fmt.Errorf("Signal %v", <-c)
	}()

	for {
		select {

		case sig := <-sigterm:
			log.Println("Exit:", sig)
			p.commitWorkSoFar()
			(*p.source).Close()

		case timestamp := <-committerTick:
			if timestamp.Sub(p.lastCommit) > *p.commitInterval {
				p.commitWorkSoFar()
				p.lastCommit = timestamp
			}
		case msg, more := <-input:
			if !more {
				log.Printf("handle: input Channel closed")
				(*p.sink).Close()
				return
			} else {
				p.lastConsumedPosition = *msg.Position
				(*p.sink).Produce(msg)
				p.copiedMessages++
				p.copiedBytes += uint64(len(*msg.Value))
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
