package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()
	const connStr = "amqp://guest:guest@localhost:5672/"
	rbtSess, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer rbtSess.Close()
	fmt.Println("Connection to message service successful.")

	ch, err := rbtSess.Channel()
	if err != nil {
		log.Fatalf("failed to open new channel: %v", err)
	}

	pubsub.DeclareAndBind(rbtSess, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug, pubsub.SimpleQueueType{Name: "durable"})
	fmt.Println("Peril gamea server connect to rabbitmqs was successful")
	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) < 1 {
			continue
		}
		elem := input[0]
		switch elem {
		case "pause":
			fmt.Println("sending pause message")
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				log.Fatal(err)
			}
		case "resume":
			fmt.Println("sending resume message")
			err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				log.Fatal(err)
			}
		case "quit":
			fmt.Println("quiting...")
			return
		default:
			fmt.Println("unknown command:", elem)

		}

	}
}
