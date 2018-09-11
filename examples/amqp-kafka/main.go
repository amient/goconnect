package main

import (
	"flag"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/io/amqp09"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
	"time"
)

var (
	//general pipleline arguments
	commitInterval = flag.Duration("commit-interval", 1*time.Second, "Commit interval of the whole connector")
	//source arguments
	uri          = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("amqp-exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("amqp-exchange-type", "direct", "Exchange type - direct|fanout|kafkaSinkTopic|x-custom")
	queue        = flag.String("amqp-queue", "test", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("amqp-key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("amqp-consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	//sink arguments
	kafkaSinkBootstrap = flag.String("kafka.bootstrap", "localhost:9092", "Kafka Bootstrap servers")
	kafkaSinkTopic     = flag.String("kafka.kafkaSinkTopic", "test", "Target Kafka Topic")
)

func main() {

	flag.Parse()

	pipeline := goc.NewPipeline()

	messages :=  pipeline.Root(&amqp09.Source {
		Uri:          *uri,
		Exchange:     *exchange,
		ExchangeType: *exchangeType,
		QueueName:    *queue,
		Group:        *consumerTag,
		BindingKey:   *bindingKey,
	})

	kvs := messages.Apply(kafka1.Encoder())

	kvs.Apply(&kafka1.Sink{
		Bootstrap: *kafkaSinkBootstrap,
		Topic: *kafkaSinkTopic,
	})

	pipeline.Run(*commitInterval)

}
