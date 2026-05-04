package pubsub

import (
	"bytes"
	"encoding/gob"
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

func jsonDecode[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func gobDecode[T any](data []byte) (T, error) {
	var v T
	b := bytes.NewBuffer(data)
	err := gob.NewDecoder(b).Decode(&v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, jsonDecode)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, gobDecode)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
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
			v, err := unmarshaller(msg.Body)
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
