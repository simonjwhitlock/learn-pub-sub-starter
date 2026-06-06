package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const connStr = "amqp://guest:guest@localhost:5672/"
	rbtSess, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer rbtSess.Close()
	fmt.Println("Connection to message service successful.")
	newChan, err := rbtSess.Channel()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get user name: %v", err)
	}
	gameState := gamelogic.NewGameState(userName)
	chName := routing.PauseKey + "." + gameState.GetUsername()

	err = pubsub.SubscribeJSON(rbtSess, routing.ExchangePerilDirect, chName, routing.PauseKey, pubsub.SimpleQueueType{Name: "transient"}, handlerPause(gameState))
	if err != nil {
		log.Fatalf("Failed to subscribe to pause feed: %v", err)
	}

	moveQueue := "army_moves." + gameState.GetUsername()
	moveKey := "army_moves.*"

	err = pubsub.SubscribeJSON(rbtSess, routing.ExchangePerilTopic, moveQueue, moveKey, pubsub.SimpleQueueType{Name: "transient"}, handlerMove(gameState))
	if err != nil {
		log.Fatalf("Failed to subscribe to move feed: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) != 0 {
			switch words[0] {
			case "spawn":
				err = gameState.CommandSpawn(words)
				if err != nil {
					log.Printf("failed to spawn: %v", err)
				}
			case "move":
				mv, err := gameState.CommandMove(words)
				if err != nil {
					log.Printf("failed to move: %v", err)
				}

				err = pubsub.PublishJSON(newChan, routing.ExchangePerilTopic, moveQueue, mv)
				if err != nil {
					log.Printf("Failed to publish move: %v", err)
				}

			case "status":
				gameState.CommandStatus()
			case "spam":
				fmt.Println("Spamming not allowed yet!")
			case "quit":
				gamelogic.PrintQuit()
				break
			default:
				fmt.Println("invalid command")
			}
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(mv gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(mv)
	}
}
