package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to bind chaneel: %v", err)
	}

	consumer, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume channel: %v", err)
	}

	go func() {
		for message := range consumer {
			var msg T
			err := json.Unmarshal(message.Body, &msg)
			if err != nil {
				message.Nack(false, false)
				fmt.Printf("failed to extract message JSON: %v", err)
				continue
			}
			handler(msg)
			if err := message.Ack(false); err != nil {
				log.Printf("failed to ACK message: %v", err)
			}
		}
	}()

	return nil
}
