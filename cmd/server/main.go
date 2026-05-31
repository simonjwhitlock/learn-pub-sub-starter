package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

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

	channel, err := rbtSess.Channel()
	if err != nil {
		log.Fatalf("failed to open new channel: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(rbtSess, "peril_topic", "game_logs", "game_logs.*", "durable")
	if err != nil {
		log.Fatalf("failed to declare or bind: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) != 0 {
			if words[0] == "pause" {
				fmt.Println("Sending Pause.")
				msg := routing.PlayingState{
					IsPaused: true,
				}

				jsonMsg, err := json.Marshal(msg)
				if err != nil {
					log.Fatalf("failed to Marshal message: %v", err)
				}

				err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, jsonMsg)
				if err != nil {
					log.Fatalf("failed to publish message: %v", err)
				}
			} else if words[0] == "resume" {
				fmt.Println("Sending Resume.")
				msg := routing.PlayingState{
					IsPaused: false,
				}

				jsonMsg, err := json.Marshal(msg)
				if err != nil {
					log.Fatalf("failed to Marshal message: %v", err)
				}

				err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, jsonMsg)
				if err != nil {
					log.Fatalf("failed to publish message: %v", err)
				}
			} else if words[0] == "quit" {
				fmt.Println("Sending Quit.")
				break
			} else {
				fmt.Println("Unrecognised command.")
			}

		}
	}

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Peril server shutting down.")
}
