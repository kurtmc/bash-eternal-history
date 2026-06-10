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
	defer close(w.ch)

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
	defer close(w.ch)

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
	defer close(w.ch)

	assert.True(t, w.Enqueue("echo hello\n"))
	waitForFlush(t, w)

	assert.Equal(t, 3, client.calls())
}

func TestWriterGivesUpAfterMaxAttempts(t *testing.T) {
	// The first line always fails; it must be dropped after maxAttempts so
	// the line behind it still gets written.
	client := &fakePutClient{errs: []error{
		errors.New("boom"), errors.New("boom"), errors.New("boom"),
	}}
	w := newTestWriter(client)
	w.maxAttempts = 3
	go w.Run()
	defer close(w.ch)

	assert.True(t, w.Enqueue("poison\n"))
	assert.True(t, w.Enqueue("healthy\n"))
	waitForFlush(t, w)

	require.Equal(t, 4, client.calls())
	content := client.input(3).Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "healthy\n", content)
}

func TestEnqueueDropsWhenQueueFull(t *testing.T) {
	w := newTestWriter(&fakePutClient{})
	w.ch = make(chan AppendHistoryMessage, 1)
	// Run() is intentionally not started, so the queue cannot drain.

	assert.True(t, w.Enqueue("a\n"))
	assert.False(t, w.Enqueue("b\n"))
	assert.Equal(t, int64(1), w.Pending())
}
