package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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

// permanentRejectionCodes are DynamoDB error codes that mean the request itself
// is malformed and can never succeed, so retrying is pointless. Everything else
// — throttling, 5xx, expired or freshly-denied credentials, a table that is
// still being created — is transient and retried indefinitely so a recoverable
// condition never silently drops history. The daemon only ever writes items it
// constructs itself (two numbers and a string well under the 400KB item limit),
// so a permanent rejection is not expected in practice; this is a backstop that
// stops a hypothetical poison line from blocking the queue forever.
var permanentRejectionCodes = map[string]struct{}{
	"ValidationException":    {},
	"SerializationException": {},
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

	mu     sync.Mutex
	closed bool
	done   chan struct{}
	// shutdownCtx is cancelled when a Shutdown deadline expires. It both stops
	// put()'s retry loop and cancels any in-flight PutItem, so the drain
	// unwinds promptly instead of waiting out a full putTimeout.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func NewHistoryWriter(svc PutItemAPI, tableName string) *HistoryWriter {
	ctx, cancel := context.WithCancel(context.Background())
	return &HistoryWriter{
		svc:       svc,
		tableName: tableName,
		// Sized so a long offline session's worth of commands fits in the
		// queue until connectivity returns.
		ch:             make(chan AppendHistoryMessage, 1000),
		putTimeout:     5 * time.Second,
		retryDelay:     5 * time.Second,
		maxAttempts:    120,
		done:           make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
}

// Pending reports how many lines are queued or currently being written.
func (w *HistoryWriter) Pending() int64 {
	return w.pending.Load()
}

// Enqueue queues a history line for writing. When the queue is full, or the
// writer has been shut down, the line is dropped instead: blocking here would
// hang the user's shell on every prompt for as long as DynamoDB is unreachable.
func (w *HistoryWriter) Enqueue(content string) bool {
	m := AppendHistoryMessage{
		Content:   content,
		Timestamp: time.Now().UnixNano(),
		// The nonce becomes the range key so that two machines writing in
		// the same nanosecond cannot overwrite each other's line.
		Nonce: rand.Int64(),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		log.Printf("WARN: history write queue is closed, dropping line")
		return false
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
	defer close(w.done)
	for m := range w.ch {
		w.put(m)
		w.pending.Add(-1)
	}
}

// Shutdown stops accepting new lines and flushes everything still queued,
// giving up once ctx is done. It returns the number of lines that could not be
// flushed before the deadline (0 on a clean drain). Safe to call more than
// once. Run must have been started, or Shutdown blocks until ctx is done.
func (w *HistoryWriter) Shutdown(ctx context.Context) int64 {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	w.mu.Unlock()

	select {
	case <-w.done:
		w.shutdownCancel()
		return 0
	case <-ctx.Done():
		// The drain did not finish in time (e.g. DynamoDB unreachable at
		// shutdown). Snapshot what is still unflushed, then cancel the retry
		// loop and any in-flight PutItem so Run can unwind instead of leaking.
		remaining := w.Pending()
		w.shutdownCancel()
		<-w.done
		return remaining
	}
}

func (w *HistoryWriter) put(m AppendHistoryMessage) {
	rejections := 0
	for {
		select {
		case <-w.shutdownCtx.Done():
			log.Printf("ERROR: shutting down, dropping unflushed history line")
			return
		default:
		}

		err := w.putOnce(m)
		if err == nil {
			return
		}

		// Only malformed-request rejections count towards giving up, so a
		// poison line the service can never accept cannot block the queue
		// forever. Transient service errors (throttling, 5xx, expired or
		// denied credentials, a table still being created) and network errors
		// are retried indefinitely: a degraded machine delays history, it must
		// not lose it.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if _, permanent := permanentRejectionCodes[apiErr.ErrorCode()]; permanent {
				rejections++
				if rejections >= w.maxAttempts {
					log.Printf("ERROR: dropping malformed history line after %d rejections: %v", rejections, err)
					return
				}
			}
		}
		log.Printf("unable to write to dynamodb, trying again: %v", err)

		select {
		case <-w.shutdownCtx.Done():
			log.Printf("ERROR: shutting down, dropping unflushed history line")
			return
		case <-time.After(w.retryDelay):
		}
	}
}

func (w *HistoryWriter) putOnce(m AppendHistoryMessage) error {
	// Derive from shutdownCtx so an in-flight PutItem is cancelled when the
	// shutdown deadline fires, not just between attempts.
	ctx, cancel := context.WithTimeout(w.shutdownCtx, w.putTimeout)
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
