package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype string

const (
	Ack         Acktype = "Ack"
	NackRequeue Acktype = "NackRequeue"
	NackDiscard Acktype = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	ach, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChannel, err := ach.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryChannel {
			var v T
			err := json.Unmarshal(msg.Body, &v)
			if err != nil {
				fmt.Printf("Error unmarshalling message: %s\n", err.Error())
				msg.Reject(false)
			} else {
				acktype := handler(v)
				switch acktype {
				case Ack:
					msg.Ack(false)
				case NackDiscard:
					msg.Nack(false, false)
				case NackRequeue:
					msg.Nack(false, true)
				}
			}
		}
	}()
	return nil
}
