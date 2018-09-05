package kafka1x

import (
	"fmt"
	"github.com/amient/goconnect/pkg"
	"github.com/confluentinc/confluent-kafka-go/kafka" //KAFKA 1.0+ client
	"log"
)

type Sink struct {
	Bootstrap string
	Topic     string
	p         *kafka.Producer
	output    chan *goconnect.Checkpoint
	input     <- chan *goconnect.Record
}

func (sink *Sink) Apply(input goconnect.RecordSource) goconnect.Sink {
	sink.input = input.Output()
	sink.output = make(chan *goconnect.Checkpoint)
	return sink
}


func (sink *Sink) Materialize() error {
	var err error
	sink.p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": sink.Bootstrap})
	if err != nil {
		return err
	}
	go func() {
		log.Printf("Kafka Sink Started")
		defer log.Printf("Kafka Sink Finished")
		defer close(sink.output)
		for inputRecord := range sink.input {
			sink.process(inputRecord)
			sink.output <- goconnect.NewCheckpoint(inputRecord.Position)
		}

	}()
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

func (sink *Sink) Join() <- chan *goconnect.Checkpoint {
	return sink.output
}


func (sink *Sink) Close() error {
	defer log.Printf("Kafka Producer shutdown OK")

	sink.p.Close()
	return nil
}


func (sink *Sink) process(record *goconnect.Record) {

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
