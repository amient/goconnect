package kafka1x

import (
	"fmt"
	"github.com/amient/goconnect/pkg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type Source struct {
	Bootstrap string
	Topic     string
	Group     string
	c         *kafka.Consumer
	closed    chan bool
	cerr	chan error
	records 	chan *goconnect.Record
}

func (source *Source) Apply() (goconnect.RecordStream) {
	source.records = make(chan *goconnect.Record)
	return source.records
}

func (source *Source) Materialize() error {
	var err error
	source.c, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": source.Bootstrap,
		"group.id":          source.Group,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset":  "earliest", //TODO pass this as config
			"enable.auto.commit": "false",
		},
		"enable.auto.commit": "false",
		"go.events.channel.enable":        true,
	})
	if err != nil {
		return err
	}
	source.closed = make(chan bool)
	source.cerr = make(chan error)
	log.Printf("Subscribing to kafka topic %s", source.Topic)
	if err := source.c.Subscribe(source.Topic, nil); err != nil {
		panic(err)
	}
	go func() {
		defer close(source.records)
		for {
			select {
			case <-source.closed:
				source.cerr <- source.c.Close()
				return
			case event := <-source.c.Events():
				switch e := event.(type) {
				case kafka.AssignedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					source.c.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					source.c.Unassign()
				case *kafka.Message:
					pos := uint64(e.TopicPartition.Offset)
					source.records <- &goconnect.Record{
						Position:  &pos,
						Key:       &e.Key,
						Value:     &e.Value,
						Timestamp: &e.Timestamp,
					}
				case kafka.PartitionEOF:
					log.Printf("%% Reached %v\n", e)
				case kafka.Error:
					source.cerr <- e
					return
				}
			}
		}
	}()
	return nil
}



func (source *Source) Commit(position uint64) {
	source.c.Commit()
}

func (source *Source) Close() error {
	source.closed <- true
	return <- source.cerr
}
