package pubsub

import (
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackDiscard
	NackRequeue
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to bind chaneel: %v", err)
	}

	consumer, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume channel: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		for message := range consumer {
			msg, err := unmarshaller(message.Body)
			if err != nil {
				message.Nack(false, false)
				fmt.Printf("failed to extract message JSON: %v\n", err)
				continue
			}
			switch handler(msg) {
			case Ack:
				message.Ack(false)
				log.Println("message acknolaged")
				fmt.Print("> ")
			case NackRequeue:
				message.Nack(false, true)
				log.Println("message not acknolaged: Requeue")
				fmt.Print("> ")
			case NackDiscard:
				message.Nack(false, false)
				log.Println("message not acknolaged: Discard")
				fmt.Print("> ")
			}
		}
	}()

	return nil
}
