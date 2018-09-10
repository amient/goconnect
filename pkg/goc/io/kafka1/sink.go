package kafka1

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"reflect"
	"time"
)

type Sink struct {
	Bootstrap   string
	Topic       string
	p           *kafka.Producer
	numProduced uint64
}

func (sink *Sink) InType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (sink *Sink) Run(input <-chan *goc.Element) {
	var err error
	sink.p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": sink.Bootstrap})
	if err != nil {
		panic(err)
	}
	for element := range input {
		kv := element.Value.(goc.KVBytes)
		err = sink.process(kv.Key, kv.Value, *element.Timestamp)
		if err != nil {
			panic(err)
		}
	}
}

func (sink *Sink) Commit(goc.Checkpoint) error {
	log.Println("Kafka Sink Commit - Number of Produced Messages", sink.numProduced)
	numNotFlushed := sink.p.Flush(15 * 1000)
	if numNotFlushed > 0 {
		return fmt.Errorf("could not flush all messages in timeout, numNotFlushed: %d", numNotFlushed)
	} else {
		sink.numProduced = 0
		return nil
	}
}

func (sink *Sink) Close() error {
	defer log.Printf("Closed Kafka Producer")
	sink.p.Close()
	return nil
}

func (sink *Sink) process(key []byte, value []byte, timestamp time.Time) error {

	//TODO Delivery report handler must be used to emit checkpoints
	go func() {
		for e := range sink.p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	sink.numProduced = sink.numProduced + 1
	// Produce messages to Topic (asynchronously)
	return sink.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink.Topic},
		Key:            key,
		Value:          value,
		Timestamp:      timestamp,
	}, nil)

}
