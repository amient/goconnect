package goc

import "time"

type Element struct {
	Timestamp  *time.Time
	Checkpoint Checkpoint
	Value      interface{}
	signal     ControlSignal
}

type ControlSignal uint8

const NoSignal ControlSignal = 0
const ControlDrain ControlSignal = 1

type InputChannel <- chan *Element

type OutputChannel chan *Element

func (o OutputChannel) Collect(e interface{}) {

}