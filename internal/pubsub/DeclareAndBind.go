package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	durable   SimpleQueueType = "durable"
	transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	var queue amqp.Queue
	var channel *amqp.Channel

	channel, err := conn.Channel()
	if err != nil {
		return channel, queue, err
	}

	if queueType == "durable" {
		queue, err = channel.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			return channel, queue, err
		}
	} else {
		queue, err = channel.QueueDeclare(queueName, false, true, true, false, nil)
		if err != nil {
			return channel, queue, err
		}
	}

	channel.QueueBind(queueName, exchange, key, false, nil)

	return channel, queue, nil

}
