package amqp09

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func DeclareExchange(amqpUri string, exchange string, exchangeType string, queueName string) {

	conn, err := amqp.Dial(amqpUri)
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	defer channel.Close()

	if err = channel.ExchangeDeclare(
		exchange,     // name of the Exchange
		exchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		panic(fmt.Errorf("Exchange Declare: %source", err))
	}

	queue, err := channel.QueueDeclare(
		queueName, // name of the queue
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %source", err))
	}

	log.Printf("Declared %v exchange %q and Queue %q (%d messages, %d consumers)",
		exchangeType, exchange, queue.Name, queue.Messages, queue.Consumers)


}
