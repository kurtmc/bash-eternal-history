package main

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakePutClient struct {
	mu     sync.Mutex
	inputs []*dynamodb.PutItemInput
	// errs[i] is returned for call i; a nil or missing entry means success.
	errs []error
}

func (f *fakePutClient) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := len(f.inputs)
	f.inputs = append(f.inputs, in)
	if call < len(f.errs) && f.errs[call] != nil {
		return nil, f.errs[call]
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakePutClient) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inputs)
}

func (f *fakePutClient) input(i int) *dynamodb.PutItemInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inputs[i]
}

func newTestWriter(client *fakePutClient) *HistoryWriter {
	w := NewHistoryWriter(client, "test")
	w.putTimeout = time.Second
	w.retryDelay = time.Millisecond
	w.maxAttempts = 5
	return w
}

func waitForFlush(t *testing.T, w *HistoryWriter) {
	t.Helper()
	require.Eventually(t, func() bool { return w.Pending() == 0 }, 5*time.Second, time.Millisecond)
}

func TestWriterWritesItem(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("echo hello\n"))
	waitForFlush(t, w)

	require.Equal(t, 1, client.calls())
	in := client.input(0)
	assert.Equal(t, "test", *in.TableName)

	content := in.Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "echo hello\n", content)

	// The timestamp must be a full int64 nanosecond value; on 32-bit builds
	// the old int-typed field wrapped around.
	ts, err := strconv.ParseInt(in.Item["timestamp"].(*types.AttributeValueMemberN).Value, 10, 64)
	require.NoError(t, err)
	assert.Greater(t, ts, int64(math.MaxInt32))

	_, err = strconv.ParseInt(in.Item["timestamp_2"].(*types.AttributeValueMemberN).Value, 10, 64)
	require.NoError(t, err)
}

func TestWriterUsesDistinctRangeKeys(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("a\n"))
	assert.True(t, w.Enqueue("b\n"))
	waitForFlush(t, w)

	require.Equal(t, 2, client.calls())
	key := func(i int) [2]string {
		in := client.input(i)
		return [2]string{
			in.Item["timestamp"].(*types.AttributeValueMemberN).Value,
			in.Item["timestamp_2"].(*types.AttributeValueMemberN).Value,
		}
	}
	// Even if both lines land in the same nanosecond, the random range key
	// keeps the primary keys distinct so neither line is overwritten.
	assert.NotEqual(t, key(0), key(1))
	assert.NotEqual(t, key(0)[0], key(0)[1])
}

func TestWriterRetriesUntilSuccess(t *testing.T) {
	client := &fakePutClient{errs: []error{errors.New("boom"), errors.New("boom")}}
	w := newTestWriter(client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("echo hello\n"))
	waitForFlush(t, w)

	assert.Equal(t, 3, client.calls())
}

func TestWriterGivesUpAfterRepeatedMalformedRejections(t *testing.T) {
	// The service keeps rejecting the first line with a malformed-request error
	// it can never accept; it must be dropped after maxAttempts so the line
	// behind it still gets written.
	rejection := &smithy.GenericAPIError{Code: "ValidationException", Message: "boom"}
	client := &fakePutClient{errs: []error{rejection, rejection, rejection}}
	w := newTestWriter(client)
	w.maxAttempts = 3
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("poison\n"))
	assert.True(t, w.Enqueue("healthy\n"))
	waitForFlush(t, w)

	require.Equal(t, 4, client.calls())
	content := client.input(3).Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "healthy\n", content)
}

func TestWriterRetriesNetworkErrorsIndefinitely(t *testing.T) {
	// Network errors do not count towards giving up: a line written while
	// offline must survive far more attempts than maxAttempts.
	var errs []error
	for range 10 {
		errs = append(errs, errors.New("dial tcp: network is unreachable"))
	}
	client := &fakePutClient{errs: errs}
	w := newTestWriter(client)
	w.maxAttempts = 3
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("echo offline\n"))
	waitForFlush(t, w)

	require.Equal(t, 11, client.calls())
	content := client.input(10).Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "echo offline\n", content)
}

func TestWriterQueueFitsAnOfflineSession(t *testing.T) {
	w := NewHistoryWriter(&fakePutClient{}, "test")
	assert.Equal(t, 1000, cap(w.ch))
}

func TestEnqueueDropsWhenQueueFull(t *testing.T) {
	w := newTestWriter(&fakePutClient{})
	w.ch = make(chan AppendHistoryMessage, 1)
	// Run() is intentionally not started, so the queue cannot drain.

	assert.True(t, w.Enqueue("a\n"))
	assert.False(t, w.Enqueue("b\n"))
	assert.Equal(t, int64(1), w.Pending())
}

func TestWriterRetriesTransientErrorsIndefinitely(t *testing.T) {
	// Throttling and credential errors are transient: they must not count
	// towards the drop budget, so a line survives far more of them than
	// maxAttempts before the service finally accepts it.
	throttle := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}
	expired := &smithy.GenericAPIError{Code: "ExpiredTokenException", Message: "creds expired"}
	client := &fakePutClient{errs: []error{throttle, throttle, expired, expired, throttle, expired}}
	w := newTestWriter(client)
	w.maxAttempts = 2
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, w.Enqueue("echo hello\n"))
	waitForFlush(t, w)

	// Six transient failures then success: none counted towards the 2-attempt
	// drop budget.
	require.Equal(t, 7, client.calls())
}

func TestShutdownDrainsQueuedLines(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(client)
	go w.Run()

	assert.True(t, w.Enqueue("a\n"))
	assert.True(t, w.Enqueue("b\n"))
	assert.True(t, w.Enqueue("c\n"))

	// A clean shutdown flushes everything still queued before returning.
	remaining := w.Shutdown(context.Background())

	assert.Equal(t, int64(0), remaining)
	assert.Equal(t, 3, client.calls())
	assert.Equal(t, int64(0), w.Pending())
}

func TestShutdownReportsUnflushedAfterDeadline(t *testing.T) {
	// DynamoDB unreachable at shutdown: the drain cannot complete, so Shutdown
	// must return once its deadline passes and report the unflushed line rather
	// than block forever.
	var errs []error
	for range 1000 {
		errs = append(errs, errors.New("dial tcp: network is unreachable"))
	}
	client := &fakePutClient{errs: errs}
	w := newTestWriter(client)
	w.retryDelay = 10 * time.Millisecond
	go w.Run()

	assert.True(t, w.Enqueue("echo offline\n"))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.Equal(t, int64(1), w.Shutdown(ctx))
}

func TestEnqueueAfterShutdownIsDropped(t *testing.T) {
	w := newTestWriter(&fakePutClient{})
	go w.Run()
	w.Shutdown(context.Background())

	// Enqueue must not panic on the closed channel; it reports the drop.
	assert.False(t, w.Enqueue("late\n"))
}
