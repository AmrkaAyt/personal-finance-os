package imports

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	parserdomain "personal-finance-os/internal/parser"
)

type RawImport struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	ImportID   string             `bson:"import_id" json:"import_id"`
	Filename   string             `bson:"filename" json:"filename"`
	SHA256     string             `bson:"sha256" json:"sha256"`
	SizeBytes  int                `bson:"size_bytes" json:"size_bytes"`
	Content    []byte             `bson:"content" json:"-"`
	Status     string             `bson:"status" json:"status"`
	ReceivedAt time.Time          `bson:"received_at" json:"received_at"`
	UpdatedAt  time.Time          `bson:"updated_at" json:"updated_at"`
}

type ParsedImport struct {
	ID           primitive.ObjectID         `bson:"_id,omitempty" json:"id"`
	ImportID     string                     `bson:"import_id" json:"import_id"`
	Filename     string                     `bson:"filename" json:"filename"`
	Status       string                     `bson:"status" json:"status"`
	Summary      parserdomain.Summary       `bson:"summary" json:"summary"`
	Transactions []parserdomain.Transaction `bson:"transactions" json:"transactions"`
	ParsedAt     time.Time                  `bson:"parsed_at" json:"parsed_at"`
	UpdatedAt    time.Time                  `bson:"updated_at" json:"updated_at"`
}

type ParseJob struct {
	ImportID   string    `json:"import_id"`
	Filename   string    `json:"filename"`
	SHA256     string    `json:"sha256"`
	SizeBytes  int       `json:"size_bytes"`
	ReceivedAt time.Time `json:"received_at"`
}

type StatementUploadedEvent struct {
	ImportID   string    `json:"import_id"`
	Filename   string    `json:"filename"`
	SHA256     string    `json:"sha256"`
	SizeBytes  int       `json:"size_bytes"`
	Status     string    `json:"status"`
	ReceivedAt time.Time `json:"received_at"`
}

type StatementParsedEvent struct {
	ImportID         string    `json:"import_id"`
	Filename         string    `json:"filename"`
	Status           string    `json:"status"`
	Format           string    `json:"format"`
	TransactionCount int       `json:"transaction_count"`
	ParsedAt         time.Time `json:"parsed_at"`
}
