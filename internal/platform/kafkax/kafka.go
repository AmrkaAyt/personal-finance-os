package kafkax

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		BatchTimeout:           200 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
}

func PublishJSON(ctx context.Context, writer *kafka.Writer, key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: body,
		Time:  time.Now().UTC(),
	})
}

func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions, replicationFactor int) error {
	if len(brokers) == 0 {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
}
