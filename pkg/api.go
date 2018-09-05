package goconnect

import "time"

type Record struct {
	Position  *uint64
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
	Commit(position uint64)
	Close() error
}

type Checkpoint struct {
	Position *uint64
	Err      error
}

type Sink interface {
	Materialize() error
	Flush() error
	Join() <-chan *Checkpoint
	Close() error
}

func NewCheckpoint(position *uint64) *Checkpoint {
	return &Checkpoint{
		Position: position,
		Err: nil,
	}

}
