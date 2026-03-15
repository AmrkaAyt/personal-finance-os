package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func Connect(url string) (*amqp.Connection, error) {
	return amqp.Dial(url)
}

func OpenChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	return conn.Channel()
}

func DeclareWorkQueue(ch *amqp.Channel, queue string) error {
	dlx := queue + ".dlx"
	dlq := queue + ".dlq"

	if err := ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(dlq, queue, dlx, false, nil); err != nil {
		return err
	}
	_, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    dlx,
		"x-dead-letter-routing-key": queue,
	})
	return err
}

func PublishJSON(ctx context.Context, ch *amqp.Channel, queue string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}
