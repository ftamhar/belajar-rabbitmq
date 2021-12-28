package main

import (
	"belajar-rabbitmq/broker"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, ch, err := broker.RabbitMQ()
	if err != nil {
		panic(err)
	}

	defer func() {
		ch.Close()
		conn.Close()
	}()

	err = ch.ExchangeDeclare("logs_direct", amqp091.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		panic(errors.Wrap(err, "failed to declare exchange"))
	}

	err = ch.Publish("logs_direct", os.Args[1], false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(os.Args[2]),
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to publish message"))
	}

	fmt.Println("Send message:", os.Args[2])
}
