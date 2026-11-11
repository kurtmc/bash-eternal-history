package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
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
	journal   *journal

	putTimeout  time.Duration
	retryDelay  time.Duration
	maxAttempts int

	mu   sync.Mutex
	wake *sync.Cond
	// queue[0] is the line being written. It stays queued until it settles.
	queue   []AppendHistoryMessage
	stalled bool
	closed  bool
	done    chan struct{}
	// shutdownCtx is cancelled when a Shutdown deadline expires. It both stops
	// put()'s retry loop and cancels any in-flight PutItem, so the drain
	// unwinds promptly instead of waiting out a full putTimeout.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// A line replayed from the journal keeps its key, so re-putting one that was
// stored just before a crash overwrites the same item.
func NewHistoryWriter(svc PutItemAPI, tableName string, j *journal) (*HistoryWriter, error) {
	queue, err := j.load()
	if err != nil {
		return nil, fmt.Errorf("could not load the write journal: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &HistoryWriter{
		svc:            svc,
		tableName:      tableName,
		journal:        j,
		putTimeout:     5 * time.Second,
		retryDelay:     5 * time.Second,
		maxAttempts:    120,
		queue:          queue,
		done:           make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
	w.wake = sync.NewCond(&w.mu)
	return w, nil
}

// Pending reports how many lines are queued or currently being written.
func (w *HistoryWriter) Pending() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return int64(len(w.queue))
}

func (w *HistoryWriter) Unflushed() []AppendHistoryMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.queue)
}

// Stalled reports whether the line being written has failed a put and not
// yet settled, which in practice means DynamoDB is unreachable.
func (w *HistoryWriter) Stalled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stalled
}

// Enqueue journals and queues a history line and returns the timestamp that
// becomes the line's hash key. After Shutdown the line is only journaled, and
// is written at the next start. It reports false only when the writer is shut
// down and the line could not be journaled either.
func (w *HistoryWriter) Enqueue(content string) (int64, bool) {
	m := AppendHistoryMessage{
		Content:   content,
		Timestamp: time.Now().UnixNano(),
		// The nonce becomes the range key so that two machines writing in
		// the same nanosecond cannot overwrite each other's line.
		Nonce: rand.Int64(),
	}
	journalErr := w.journal.add(m)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		if journalErr != nil {
			slog.Error("writer is shut down and the line could not be journaled, refusing it", slog.Any("error", journalErr))
			return m.Timestamp, false
		}
		return m.Timestamp, true
	}
	if journalErr != nil {
		slog.Warn("could not journal history line, it is lost if the daemon stops before it is written", slog.Any("error", journalErr))
	}
	w.queue = append(w.queue, m)
	w.wake.Signal()
	return m.Timestamp, true
}

// Run returns once Shutdown is called and the queue is empty, or its deadline passes.
func (w *HistoryWriter) Run() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.closed {
			w.wake.Wait()
		}
		if len(w.queue) == 0 {
			w.mu.Unlock()
			return
		}
		m := w.queue[0]
		w.mu.Unlock()

		if !w.put(m) {
			return
		}
		if err := w.journal.remove(m); err != nil {
			slog.Warn("could not remove written line from the write journal, it will be written again at the next start", slog.Any("error", err))
		}

		w.mu.Lock()
		w.queue = w.queue[1:]
		w.stalled = false
		w.mu.Unlock()
	}
}

// Shutdown stops accepting new lines and flushes everything still queued,
// giving up once ctx is done. It returns the number of lines left unflushed
// (0 on a clean drain), which stay in the journal. Safe to call more than
// once. Run must have been started, or Shutdown never returns.
func (w *HistoryWriter) Shutdown(ctx context.Context) int64 {
	w.mu.Lock()
	w.closed = true
	w.wake.Broadcast()
	w.mu.Unlock()

	select {
	case <-w.done:
	case <-ctx.Done():
	}
	// Cancel the retry loop and any in-flight PutItem so Run can unwind
	// instead of leaking.
	w.shutdownCancel()
	<-w.done
	return w.Pending()
}

// put writes m and reports whether it settled: stored, or rejected for good
// and dropped. It returns false when the shutdown deadline interrupts it.
func (w *HistoryWriter) put(m AppendHistoryMessage) bool {
	rejections := 0
	for {
		if w.shutdownCtx.Err() != nil {
			return false
		}

		err := w.putOnce(m)
		if err == nil {
			return true
		}
		w.mu.Lock()
		w.stalled = true
		w.mu.Unlock()

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
					return true
				}
			}
		}
		log.Printf("unable to write to dynamodb, trying again: %v", err)

		select {
		case <-w.shutdownCtx.Done():
			return false
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
