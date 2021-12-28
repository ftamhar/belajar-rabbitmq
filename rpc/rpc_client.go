package main

import (
	"belajar-rabbitmq/broker"
	"log"
	"os"

	"github.com/google/uuid"
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

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		panic(errors.Wrap(err, "failed to declare queue"))
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		panic(errors.Wrap(err, "failed to consume queue"))
	}

	corrID := uuid.NewString()

	err = ch.Publish("", "rpc_queue", false, false, amqp091.Publishing{
		ContentType:   "text/plain",
		CorrelationId: corrID,
		ReplyTo:       q.Name,
		Body:          []byte(os.Args[1]),
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to publish message"))
	}

	log.Println("Send Message :", os.Args[1])
	log.Println("Waiting for response...")

	for d := range msgs {
		if d.CorrelationId == corrID {
			log.Println("Result :", string(d.Body))
			break
		}
	}
}
