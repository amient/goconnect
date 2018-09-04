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

	//declared pipeline stages (no i/o happens at this point, only channels are chained)
	var source goconnect.Source = &kafka1x.Source{
		Bootstrap: *kafkaSourceBootstrap,
		Topic:     *kafkaSourceTopic,
		Group:     *kafkaSourceGroup,
	}

	var sink = kafka1x.Sink{Bootstrap: *kafkaSinkBootstrap, Topic: *kafkaSinkTopic}.Apply(source)

	//materialize and run the pipeline (this opens the connections to the respective backends)
	goconnect.Execute(source, sink, *commitInterval)


}
