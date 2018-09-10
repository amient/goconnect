package kafka1

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"reflect"
)

type ConsumerCheckpoint struct {
	Partition int32
	Offset    uint64
}

type Source struct {
	Bootstrap string
	Topic     string
	Group     string
	consumer  *kafka.Consumer
}

func (source *Source) OutType() reflect.Type {
	return reflect.TypeOf(goc.KVBytes{})
}

func (source *Source) Run(output chan *goc.Element) {
	var err error
	source.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": source.Bootstrap,
		"group.id":          source.Group,
		"default.topic.config": kafka.ConfigMap{
			"auto.offset.reset":  "earliest", //TODO pass this as config
			"enable.auto.commit": "false",
		},
		"enable.auto.commit":       "false",
		"go.events.channel.enable": true,
	})
	if err != nil {
		panic(err)
	}

	log.Printf("Subscribing to kafka topic %s", source.Topic)
	if err := source.consumer.Subscribe(source.Topic, nil); err != nil {
		panic(err)
	}

	for event := range source.consumer.Events() {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			source.consumer.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			source.consumer.Unassign()
		case *kafka.Message:
			output <- &goc.Element{
				Timestamp: &e.Timestamp,
				Checkpoint: &ConsumerCheckpoint{
					Partition: e.TopicPartition.Partition,
					Offset:    uint64(e.TopicPartition.Offset),
				},
				Value: goc.KVBytes{
					Key:   e.Key,
					Value: e.Value,
				},
			}
		case kafka.PartitionEOF:
			log.Printf("%% Reached %v\n", e)
		case kafka.Error:
			panic(e)
		}
	}

}

func (source *Source) Commit(checkpoint goc.Checkpoint) error {
	log.Println(fmt.Errorf("kafka source commit not implemented"))
	//TODO need to commit
	return nil
}

func (source *Source) Close() error {
	defer log.Println("Closed Kafka Consumer")
	return source.consumer.Close()
}
