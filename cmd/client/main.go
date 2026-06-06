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
	fmt.Println("Starting Peril client...")
	const connStr = "amqp://guest:guest@localhost:5672/"
	rbtSess, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer rbtSess.Close()
	fmt.Println("Connection to message service successful.")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("Failed to get user name: %v", err)
	}
	gameState := gamelogic.NewGameState(userName)
	chName := routing.PauseKey + "." + gameState.GetUsername()

	err = pubsub.SubscribeJSON(rbtSess, routing.ExchangePerilDirect, chName, routing.PauseKey, pubsub.SimpleQueueType{Name: "transient"}, handlerPause(gameState))
	if err != nil {
		log.Fatal(err)
	}
	moveKey := "army_moves." + gameState.GetUsername()
	pubsub.DeclareAndBind(rbtSess, routing.ExchangePerilTopic, moveKey, routing.GameLogSlug, pubsub.SimpleQueueType{Name: "transient"})

	for {
		words := gamelogic.GetInput()
		if len(words) != 0 {
			if words[0] == "spawn" {
				gameState.CommandSpawn(words)
			} else if words[0] == "move" {
				gameState.CommandMove(words)
			} else if words[0] == "status" {
				gameState.CommandStatus()
			} else if words[0] == "help" {
				gamelogic.PrintClientHelp()
			} else if words[0] == "spam" {
				fmt.Println("Spamming not allowed yet!")
			} else if words[0] == "quit" {
				gamelogic.PrintQuit()
				break
			} else {
				fmt.Println("Error: Unknown command.")
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
