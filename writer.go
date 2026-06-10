package main

import (
	"context"
	"log"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// PutItemAPI is the subset of the DynamoDB API used to store history lines.
type PutItemAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type AppendHistoryMessage struct {
	Content   string
	Timestamp int64
	Nonce     int64
}

// HistoryWriter persists history lines to DynamoDB asynchronously so that
// writes from the shell never wait on the network.
type HistoryWriter struct {
	svc       PutItemAPI
	tableName string
	ch        chan AppendHistoryMessage

	putTimeout  time.Duration
	retryDelay  time.Duration
	maxAttempts int

	pending atomic.Int64
}

func NewHistoryWriter(svc PutItemAPI, tableName string) *HistoryWriter {
	return &HistoryWriter{
		svc:         svc,
		tableName:   tableName,
		ch:          make(chan AppendHistoryMessage, 100),
		putTimeout:  5 * time.Second,
		retryDelay:  5 * time.Second,
		maxAttempts: 120,
	}
}

// Pending reports how many lines are queued or currently being written.
func (w *HistoryWriter) Pending() int64 {
	return w.pending.Load()
}

// Enqueue queues a history line for writing. When the queue is full the line
// is dropped instead: blocking here would hang the user's shell on every
// prompt for as long as DynamoDB is unreachable.
func (w *HistoryWriter) Enqueue(content string) bool {
	m := AppendHistoryMessage{
		Content:   content,
		Timestamp: time.Now().UnixNano(),
		// The nonce becomes the range key so that two machines writing in
		// the same nanosecond cannot overwrite each other's line.
		Nonce: rand.Int64(),
	}
	w.pending.Add(1)
	select {
	case w.ch <- m:
		return true
	default:
		w.pending.Add(-1)
		log.Printf("WARN: history write queue is full, dropping line")
		return false
	}
}

// Run consumes the queue until the channel is closed.
func (w *HistoryWriter) Run() {
	for m := range w.ch {
		w.put(m)
		w.pending.Add(-1)
	}
}

func (w *HistoryWriter) put(m AppendHistoryMessage) {
	for attempt := 1; ; attempt++ {
		err := w.putOnce(m)
		if err == nil {
			return
		}
		if attempt >= w.maxAttempts {
			// Give up rather than let one unwritable line block every
			// history line behind it forever.
			log.Printf("ERROR: dropping history line after %d attempts: %v", attempt, err)
			return
		}
		log.Printf("unable to write to dynamodb, trying again: %v", err)
		time.Sleep(w.retryDelay)
	}
}

func (w *HistoryWriter) putOnce(m AppendHistoryMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.putTimeout)
	defer cancel()
	_, err := w.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &w.tableName,
		Item: map[string]types.AttributeValue{
			"timestamp":   &types.AttributeValueMemberN{Value: strconv.FormatInt(m.Timestamp, 10)},
			"timestamp_2": &types.AttributeValueMemberN{Value: strconv.FormatInt(m.Nonce, 10)},
			"content":     &types.AttributeValueMemberS{Value: m.Content},
		},
	})
	return err
}
