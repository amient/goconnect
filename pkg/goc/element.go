package goc

import "time"


type Element struct {
	signal     ControlSignal
	Timestamp  *time.Time
	Checkpoint Checkpoint
	Value      interface{}
}

func (element *Element) hasSignal() bool {
	return element.signal != NoSignal
}

type ControlSignal uint8

const NoSignal ControlSignal = 0
const ControlDrain ControlSignal = 1

