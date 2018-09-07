package goc

import (
	"log"
)

type Control struct {
	signals chan *ControlSignal
}

func (control *Control) Send(signal ControlSignal) {
	control.signals <- &signal
}

func (control *Control) Signals(output chan *Element) <-chan *ControlSignal {
	result := make(chan *ControlSignal)
	go func() {
		defer close(result)
		for c := range control.signals {
			switch *c {
			case ControlDrain:
				output <- &Element{signal: ControlDrain}
				for done := false; !done; {
					signal, _ := <-control.signals
					done = *signal == ControlResume
				}
			case ControlCleanStop:
				log.Println("Control Clean Stop")
				output <- &Element{signal: ControlDrain}
				output <- &Element{signal: ControlCleanStop}
				close(output)
			}

		}
	}()
	return result
}

func (control *Control) Close() {
	log.Println("Closing Control Channel")
	close(control.signals)
}

type ControlSignal uint8

const NoSignal ControlSignal = 0
const ControlDrain ControlSignal = 1
const ControlResume ControlSignal = 2
const ControlCleanStop ControlSignal = 3
