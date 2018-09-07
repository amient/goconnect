package goc

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Pipeline struct {
	outputs				 []*Stream
	commitInterval       time.Duration
	lastCommit           time.Time
	numProcessedElements uint32
	lastConsumedPosition interface{}
}

func (p *Pipeline) Run(outputs ...*Stream) error {

	p.outputs = outputs
	log.Printf("Materializing and Running Pipeline of %d outputs\n", len(p.outputs))
	for _, stream := range outputs {
		stream.Materialize()
	}

	//open committer tick underlying
	if p.commitInterval == 0 {
		p.commitInterval = 5 * time.Second
		log.Printf("Using default commit interval %q\n", p.commitInterval)
	}
	committerTick := time.NewTicker(p.commitInterval).C

	//open termination signal underlying
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
			case _, more := <-stream.underlying:
				if !more {
					log.Printf("Source underlying Terminated")
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

//func (p *Pipeline) Root(t Transform) *Stream {
//	if method, exists := reflect.TypeOf(t).MethodByName("Fn"); !exists {
//		panic(fmt.Errorf("transform must provide Fn method"))
//	} else if method.Type.NumOut() != 0 {
//		panic(fmt.Errorf("root transform must provide Fn method with no outputs"))
//	} else if method.Type.NumIn() != 2 {
//		panic(fmt.Errorf("root transform must provide Fn method with 1 argument"))
//	} else {
//		chanType := method.Type.In(1)
//		fmt.Println(chanType.Elem())
//		return &Stream{
//			Type: chanType.Elem(),
//			underlying : make (chan interface{}),
//			generator: &Fn {
//
//			},
//			transform: t,
//		}
//	}
//}
