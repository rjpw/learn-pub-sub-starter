package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("Error creating broker channel")
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(username)

	// ------------- LISTEN FOR PAUSE MESSAGES -----------------------
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState))

	if err != nil {
		fmt.Println(fmt.Errorf("Unable to subscribe for pause messages: %w", err))
		os.Exit(1)
	}

	// ------------- LISTEN FOR MOVE MESSAGES -----------------------
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(gameState, ch))

	if err != nil {
		fmt.Println(fmt.Errorf("Unable to subscribe for move messages: %w", err))
		os.Exit(1)
	}

	// ------------- LISTEN FOR WAR MESSAGES -----------------------
	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		"war",
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		handlerWar(gameState, ch))

	if err != nil {
		fmt.Println(fmt.Errorf("Unable to subscribe for war messages: %w", err))
		os.Exit(1)
	}

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

			mv, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(fmt.Errorf("%w", err))
			} else {
				fmt.Printf("%v\n", strings.Join(input, " "))
			}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), mv)
			if err != nil {
				fmt.Println(fmt.Errorf("%w", err))
			} else {
				fmt.Println("Move published successfully")
			}

		case "status":
			gameState.CommandStatus()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Usage: spam <n>")
			} else {
				i, err := strconv.Atoi(input[1])
				if err != nil {
					fmt.Printf("Error parsing int from %s\n", input[1])
				}
				if i < 1 {
					fmt.Println("Spam count must be greater than 0")
				}
				for range i {
					msg := gamelogic.GetMaliciousLog()
					err = pubsub.PublishGob(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.GameLogSlug, username), routing.GameLog{
						CurrentTime: time.Now(),
						Message:     msg,
						Username:    username,
					})
				}
			}
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, replyChannel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(mv gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(mv)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			fmt.Printf("Message disposition: %s\n", pubsub.Ack)
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(replyChannel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()),
				gamelogic.RecognitionOfWar{
					Attacker: mv.Player,
					Defender: gs.GetPlayerSnap(),
				})
			if err != nil {
				fmt.Printf("Message disposition: %s\n", pubsub.NackRequeue)
				return pubsub.NackRequeue
			}
			fmt.Printf("Message disposition: %s\n", pubsub.Ack)
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fallthrough
		default:
			fmt.Printf("Message disposition: %s\n", pubsub.NackDiscard)
			return pubsub.NackDiscard
		}
	}
}

func outcomeMessage(outcome gamelogic.WarOutcome, winner, loser string) string {
	switch outcome {
	case gamelogic.WarOutcomeNotInvolved:
		return ""
	case gamelogic.WarOutcomeNoUnits:
		return ""
	case gamelogic.WarOutcomeOpponentWon:
		fallthrough
	case gamelogic.WarOutcomeYouWon:
		return fmt.Sprintf("%s won a war against %s", winner, loser)
	case gamelogic.WarOutcomeDraw:
		return fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
	default:
		return ""
	}
}

func publishLog(msg, username string, logChannel *amqp.Channel) error {
	err := pubsub.PublishGob(logChannel,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     msg,
			Username:    username,
		})

	if err != nil {
		fmt.Printf("Error sending log: %s\n", err.Error())
		return err
	}

	return nil
}

func handlerWar(gs *gamelogic.GameState, replyChannel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(rw)
		msg := outcomeMessage(outcome, winner, loser)
		err := publishLog(msg, gs.GetUsername(), replyChannel)
		if err != nil {
			return pubsub.NackRequeue
		} else {
			return pubsub.Ack
		}

		// switch outcome {
		// case gamelogic.WarOutcomeNotInvolved:
		// 	return pubsub.NackRequeue
		// case gamelogic.WarOutcomeNoUnits:
		// 	return pubsub.NackDiscard
		// case gamelogic.WarOutcomeOpponentWon:
		// 	fallthrough
		// case gamelogic.WarOutcomeYouWon:
		// 	fallthrough
		// case gamelogic.WarOutcomeDraw:
		// 	return pubsub.Ack
		// default:
		// 	return pubsub.NackDiscard
		// }
	}
}
