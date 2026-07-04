package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QOS: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
			}
			ackResponse := handler(target)
			switch ackResponse {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			}
		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var target T
		err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&target)
		return target, err
	})
}
