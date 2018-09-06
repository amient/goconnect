package goconnect

import "time"

type Record struct {
	Position  interface {}
	Key       *[]byte
	Value     *[]byte
	Timestamp *time.Time
}

type RecordSource interface {
	Output() <-chan *Record
}

type Source interface {
	Output() <-chan *Record
	Materialize() error
	Commit(position interface {})
	Close() error
}

type Checkpoint struct {
	Position interface {}
	Err      error
}

type Sink interface {
	Materialize() error
	Flush() error
	Join() <-chan *Checkpoint
	Close() error
}

func NewCheckpoint(position interface {}) *Checkpoint {
	return &Checkpoint{
		Position: position,
		Err: nil,
	}

}
