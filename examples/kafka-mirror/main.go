package main

import (
	"flag"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/io/kafka1x"
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

	//declared pipeline stages (pure structs, no i/o happens at this point)
	var source goconnect.Source = &kafka1x.Source{
		Bootstrap: *kafkaSourceBootstrap,
		Topic:     *kafkaSourceTopic,
		Group:     *kafkaSourceGroup,
	}
	var sink goconnect.Sink = &kafka1x.Sink{
		Bootstrap: *kafkaSinkBootstrap,
		Topic:     *kafkaSinkTopic,
	}

	//initialize pipeline (this opens the connections to the respective backends)
	pipeline := goconnect.CreatePipeline(&source, &sink, commitInterval)

	//start streaming and processing the data with at-least-once guarantees
	pipeline.Run()

}
