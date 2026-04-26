package kafkax

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"personal-finance-os/internal/eventcontracts"
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

func PublishContractJSON(ctx context.Context, writer *kafka.Writer, key string, contract eventcontracts.Contract, payload any) error {
	if err := eventcontracts.ValidatePayload(contract, payload); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(key),
		Value:   body,
		Headers: ContractHeaders(contract),
		Time:    time.Now().UTC(),
	})
}

func ContractHeaders(contract eventcontracts.Contract) []kafka.Header {
	return []kafka.Header{
		{Key: "x-event-type", Value: []byte(contract.Type)},
		{Key: "x-event-version", Value: []byte(contract.Version)},
	}
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
