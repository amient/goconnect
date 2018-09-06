package amqp_stdout

import (
	"flag"
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

	//flag.Parse()
	//
	////declared pipeline stages (no i/o happens at this point, only channels are chained)
	//pipeline := new(goc.Pipeline)
	//
	//messages :=  pipeline.Root(amqp09.Source {
	//	Uri:          *uri,
	//	Exchange:     *exchange,
	//	ExchangeType: *exchangeType,
	//	QueueName:    *queue,
	//	Group:        *consumerTag,
	//	BindingKey:   *bindingKey,
	//})
	//
	//pipeline.Run(5 * time.Second)
	//goc.RunPipeline(5 * time.Second, messages.Apply(std.StdOutSink()))

}