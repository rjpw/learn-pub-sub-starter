package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	fmt.Println("Starting Peril server...")
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

	//pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)

	err := pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable, publishLog())

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating broker channel")
		os.Exit(1)
	}

	gamelogic.PrintServerHelp()

	for {

		input := gamelogic.GetInput()
		var command string
		if len(input) > 0 {
			command = input[0]
		}

		switch command {
		case "pause":
			fmt.Println("Sending pause message")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message")
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting the game")
			return
		case "help":
			gamelogic.PrintServerHelp()
		case "":
			// do nothing
		default:
			fmt.Printf("Unrecognized command: \"%s\"\n\n", command)
			gamelogic.PrintServerHelp()
		}

	}

}

func publishLog() func(gl routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
