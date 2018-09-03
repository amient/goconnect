package goconnect

import "time"

type Record struct {
	Position  *uint64
	Key       *[]byte
	Value     *[]byte
	Timestamp *time.Time
}

type Source interface {
	Initialize() error
	Records() (<-chan *Record)
	Commit(position uint64)
	Close() error
}

type Sink interface {
	Initialize() error
	Produce(record *Record)
	Flush() error
	Close() error
}



