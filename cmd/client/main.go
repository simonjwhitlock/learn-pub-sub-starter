package main

import (
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
	chName := fmt.Sprintf("%v.%v", routing.PauseKey, userName)
	_, _, err = pubsub.DeclareAndBind(rbtSess, "peril_direct", chName, routing.PauseKey, "transient")
	if err != nil {
		log.Fatalf("failed to declare or bind: %v", err)
	}

	GameState := gamelogic.NewGameState(userName)

	for {
		words := gamelogic.GetInput()
		if len(words) != 0 {
			if words[0] == "spawn" {
				GameState.CommandSpawn(words)
			} else if words[0] == "move" {
				GameState.CommandMove(words)
			} else if words[0] == "status" {
				GameState.CommandStatus()
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

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Peril server shutting down.")
}
