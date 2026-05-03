package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	fmt.Println("Starting Peril client...")
	amqpURI := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		fmt.Println(fmt.Errorf("Error connecting to message broker: %w", err))
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to broker.")

	startREPL(conn)

}

func startREPL(conn *amqp.Connection) {

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(fmt.Errorf("Cannot continue: %w", err))
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(username)

	// ch, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, pubsub.Transient)
	// if err != nil {
	// 	fmt.Println(fmt.Errorf("Cannot declaring queue: %w", err))
	// 	os.Exit(1)
	// }

	pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, pubsub.Transient)

	for {

		input := gamelogic.GetInput()
		var command string
		if len(input) > 0 {
			command = input[0]
		}

		switch command {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(fmt.Errorf("%w", err))
			}
		case "move":
			_, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(fmt.Errorf("%w", err))
			} else {
				fmt.Printf("%v\n", strings.Join(input, " "))
			}
		case "status":
			gameState.CommandStatus()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		case "help":
			gamelogic.PrintClientHelp()
		case "":
			// do nothing
		default:
			fmt.Printf("Unrecognized command: \"%s\"\n\n", command)
			gamelogic.PrintServerHelp()
		}

	}

}
