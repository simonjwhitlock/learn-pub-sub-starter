package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType struct {
	Name string
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not form conneciton")
	}
	durable := true
	autoDelete := false
	exclusive := false
	switch queueType.Name {
	case "durable":
		durable = true
		autoDelete = false
		exclusive = false

	case "transient":
		durable = false
		autoDelete = true
		exclusive = true
	}
	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind exchange to queue")
	}
	return ch, queue, nil
}
