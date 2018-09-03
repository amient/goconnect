package kafka1x

import (
	"fmt"
	"github.com/amient/goconnect"
	"github.com/confluentinc/confluent-kafka-go/kafka" //KAFKA 1.0+ client
	"log"
)

type Sink struct {
	Bootstrap string
	Topic     string
	p         *kafka.Producer
}

func (sink *Sink) Initialize() error {
	var err error
	sink.p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": sink.Bootstrap})
	if err != nil {
		return err
	}
	return nil
}

func (sink *Sink) Close() error {
	defer log.Printf("Kafka Producer shutdown OK")
	sink.p.Close()
	return nil
}

func (sink *Sink) Flush() error {
	numUnflushed := sink.p.Flush(15 * 1000)
	if numUnflushed > 0 {
		return fmt.Errorf("could not flush all messages in timeout, numUnflushed: %d", numUnflushed)
	} else {
		return nil
	}
}

func (sink *Sink) Produce(record *goconnect.Record) {

	// Delivery report handler for produced messages
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

	// Produce messages to Topic (asynchronously)
	sink.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &sink.Topic},
		Key:            *record.Key,
		Value:          *record.Value,
		Timestamp:		*record.Timestamp,
	}, nil)

}
