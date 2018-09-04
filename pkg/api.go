package goconnect

import "time"

type Record struct {
	Position  *uint64
	Key       *[]byte
	Value     *[]byte
	Timestamp *time.Time
}

type RecordStream <-chan *Record

type RecordSource interface {
	Apply() RecordStream
}

type Decoder interface {
	Apply(input RecordSource) Decoder
}

type Source interface {
	Apply() RecordStream
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
