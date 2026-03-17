package mongox

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

func EnsureUniqueIndex(ctx context.Context, collection *mongo.Collection, field string) error {
	_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: field, Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func EnsureUniqueCompoundIndex(ctx context.Context, collection *mongo.Collection, fields ...string) error {
	keys := make(bson.D, 0, len(fields))
	for _, field := range fields {
		keys = append(keys, bson.E{Key: field, Value: 1})
	}
	_, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(true),
	})
	return err
}

func DropIndex(ctx context.Context, collection *mongo.Collection, name string) error {
	if name == "" {
		return nil
	}
	_, err := collection.Indexes().DropOne(ctx, name)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	if err != nil && err == mongo.ErrNilDocument {
		return nil
	}
	if err != nil && strings.Contains(err.Error(), "index not found") {
		return nil
	}
	return err
}
