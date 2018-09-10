package main

import (
	"flag"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
	"time"
)

var (
	//general pipleline arguments
	commitInterval = flag.Duration("commit-interval", 5*time.Second, "Commit interval of the whole connector")
	//
	kafkaSourceBootstrap = flag.String("kafka.source.bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	kafkaSourceGroup     = flag.String("kafka.source.group", "goconnect-mirror", "Source Kafka Consumer Group")
	kafkaSourceTopic     = flag.String("kafka.source.topic", "test", "Source Kafka Topic")
	kafkaSinkBootstrap   = flag.String("kafka.sink.bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaSinkTopic       = flag.String("kafka.sink.topic", "test-copy", "Destination Kafka Topic")
)

func main() {

	pipeline := goc.NewPipeline()

	source := pipeline.Root(&kafka1.Source{
		Bootstrap: *kafkaSourceBootstrap,
		Topic:     *kafkaSourceTopic,
		Group:     *kafkaSourceGroup,
	})

	source.Apply(&kafka1.Sink{
		Bootstrap: *kafkaSinkBootstrap,
		Topic: *kafkaSinkTopic,
	})

	pipeline.Run(*commitInterval)

}
