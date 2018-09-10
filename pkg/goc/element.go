package goc

import "time"

type Element struct {
	Timestamp  *time.Time
	Checkpoint Checkpoint
	Value      interface{}
	Signal     ControlSignal //FIXME this should not be exported but it is used to complete bounded sources atm
}

type ControlSignal uint8

const NoSignal ControlSignal = 0
const ControlDrain ControlSignal = 1
