package main

import (
	"belajar-rabbitmq/broker"
	"flag"
	"fmt"

	"github.com/pkg/errors"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	negara := flag.String("n", "Indonesia", "Nama Negara")
	provinsi := flag.String("p", "Jakarta", "Nama Provinsi")
	message := flag.String("m", "Hello World!", "Pesan")
	flag.Parse()

	conn, ch, err := broker.RabbitMQ()
	if err != nil {
		panic(err)
	}

	defer func() {
		ch.Close()
		conn.Close()
	}()

	err = ch.ExchangeDeclare("logs_header", amqp091.ExchangeHeaders, true, false, false, false, nil)
	if err != nil {
		panic(errors.Wrap(err, "failed to declare exchange"))
	}

	err = ch.Publish("logs_header", "", false, false, amqp091.Publishing{
		Headers: map[string]interface{}{
			"negara":   *negara,
			"provinsi": *provinsi,
		},
		ContentType: "text/plain",
		Body:        []byte(*message),
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to publish message"))
	}

	fmt.Println("Send message:", *message)
}
