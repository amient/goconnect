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
	producer    *kafka.Producer
	numProduced uint64
	deliveries  chan kafka.Event
}

func (sink *Sink) InType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (sink *Sink) Run(input <-chan *goc.Element) {
	var err error
	sink.producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": sink.Bootstrap,
	})
	sink.deliveries = make(chan kafka.Event)
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

func (sink *Sink) Flush(*goc.Checkpoint) error {
	if sink.numProduced > 0 {
		log.Println("Kafka Sink Commit - Number of Produced Messages", sink.numProduced)
		numNotFlushed := sink.producer.Flush(15 * 1000)
		if numNotFlushed > 0 {
			return fmt.Errorf("could not flush all messages in timeout, numNotFlushed: %d", numNotFlushed)
		} else {
			sink.numProduced = 0
		}
	}
	return nil
}

func (sink *Sink) Close() error {
	defer log.Printf("Closed Kafka Producer")
	sink.producer.Close()
	close(sink.deliveries)
	return nil
}

func (sink *Sink) process(key []byte, value []byte, timestamp time.Time) error {

	go func() {
		for e := range sink.deliveries {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					//TODO use delivery reports for exactly-once processing guarantees
				}
			}
		}
	}()

	defer sink.updateCounter()
	return sink.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink.Topic},
		Key:            key,
		Value:          value,
		Timestamp:      timestamp,
	}, sink.deliveries)

}

func (sink *Sink) updateCounter() {
	sink.numProduced++
}