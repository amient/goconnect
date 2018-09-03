package main

import (
	"flag"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/io/amqp091"
	"github.com/amient/goconnect/io/kafka1x"
	"time"
)

var (
	//general pipleline arguments
	commitInterval = flag.Duration("commit-interval", 5*time.Second, "Commit interval of the whole connector")
	//source arguments
	uri          = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("amqp-exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("amqp-exchange-type", "direct", "Exchange type - direct|fanout|kafkaTopic|x-custom")
	queue        = flag.String("amqp-queue", "test", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("amqp-key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("amqp-consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	//sink arguments
	kafkaBootstrap = flag.String("kafka.bootstrap", "localhost:9092", "Kafka Bootstrap servers")
	kafkaTopic     = flag.String("kafka.kafkaTopic", "test", "Target Kafka Topic")
)

func main() {

	flag.Parse()

	//declared pipeline stages (pure structs, no i/o happens at this point)
	var source goconnect.Source = &amqp091.Source{
		Uri:          *uri,
		Exchange:     *exchange,
		ExchangeType: *exchangeType,
		QueueName:    *queue,
		Group:        *consumerTag,
		BindingKey:   *bindingKey,
	}
	var sink goconnect.Sink = &kafka1x.Sink{
		Bootstrap: *kafkaBootstrap,
		Topic:     *kafkaTopic,
	}

	//initialize pipeline (this opens the connections to the respective backends)
	pipeline := goconnect.CreatePipeline(&source, &sink, commitInterval)

	//start streaming and processing the data with at-least-once guarantees
	pipeline.Run()

}
