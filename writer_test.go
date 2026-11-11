package main

import (
	"context"
	"errors"
	"math"
	"os"
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
	stored []*dynamodb.PutItemInput
	// errs[i] is returned for call i; a nil or missing entry means success.
	errs    []error
	offline bool
}

func (f *fakePutClient) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := len(f.inputs)
	f.inputs = append(f.inputs, in)
	if f.offline {
		return nil, errors.New("dial tcp: network is unreachable")
	}
	if call < len(f.errs) && f.errs[call] != nil {
		return nil, f.errs[call]
	}
	f.stored = append(f.stored, in)
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakePutClient) setOffline(offline bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offline = offline
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

func (f *fakePutClient) storedContents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var contents []string
	for _, in := range f.stored {
		contents = append(contents, in.Item["content"].(*types.AttributeValueMemberS).Value)
	}
	return contents
}

type itemKey struct {
	timestamp, nonce string
}

func keyOf(in *dynamodb.PutItemInput) itemKey {
	return itemKey{
		timestamp: in.Item["timestamp"].(*types.AttributeValueMemberN).Value,
		nonce:     in.Item["timestamp_2"].(*types.AttributeValueMemberN).Value,
	}
}

func newTestWriter(t *testing.T, client *fakePutClient) *HistoryWriter {
	t.Helper()
	return openTestWriter(t, client, t.TempDir())
}

func openTestWriter(t *testing.T, client *fakePutClient, journalDir string) *HistoryWriter {
	t.Helper()
	j, err := openJournal(journalDir)
	require.NoError(t, err)
	w, err := NewHistoryWriter(client, "test", j)
	require.NoError(t, err)
	w.putTimeout = time.Second
	w.retryDelay = time.Millisecond
	w.maxAttempts = 5
	return w
}

func journaled(t *testing.T, journalDir string) []AppendHistoryMessage {
	t.Helper()
	j, err := openJournal(journalDir)
	require.NoError(t, err)
	lines, err := j.load()
	require.NoError(t, err)
	return lines
}

func waitForFlush(t *testing.T, w *HistoryWriter) {
	t.Helper()
	require.Eventually(t, func() bool { return w.Pending() == 0 }, 5*time.Second, time.Millisecond)
}

func enqueue(t *testing.T, w *HistoryWriter, content string) bool {
	t.Helper()
	_, ok := w.Enqueue(content)
	return ok
}

func TestEnqueueReturnsStoredTimestamp(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(t, client)
	go w.Run()
	defer w.Shutdown(context.Background())

	timestamp, ok := w.Enqueue("echo hello\n")
	require.True(t, ok)
	waitForFlush(t, w)

	stored := client.input(0).Item["timestamp"].(*types.AttributeValueMemberN).Value
	assert.Equal(t, strconv.FormatInt(timestamp, 10), stored)
}

func TestWriterWritesItem(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(t, client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "echo hello\n"))
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
	w := newTestWriter(t, client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "a\n"))
	assert.True(t, enqueue(t, w, "b\n"))
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
	w := newTestWriter(t, client)
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "echo hello\n"))
	waitForFlush(t, w)

	assert.Equal(t, 3, client.calls())
}

func TestWriterGivesUpAfterRepeatedMalformedRejections(t *testing.T) {
	// The service keeps rejecting the first line with a malformed-request error
	// it can never accept; it must be dropped after maxAttempts so the line
	// behind it still gets written.
	rejection := &smithy.GenericAPIError{Code: "ValidationException", Message: "boom"}
	client := &fakePutClient{errs: []error{rejection, rejection, rejection}}
	dir := t.TempDir()
	w := openTestWriter(t, client, dir)
	w.maxAttempts = 3
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "poison\n"))
	assert.True(t, enqueue(t, w, "healthy\n"))
	waitForFlush(t, w)

	require.Equal(t, 4, client.calls())
	content := client.input(3).Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "healthy\n", content)
	assert.Empty(t, journaled(t, dir), "the dropped line must not be replayed at the next start")
	assert.False(t, w.Stalled())
}

func TestWriterRetriesNetworkErrorsIndefinitely(t *testing.T) {
	// Network errors do not count towards giving up: a line written while
	// offline must survive far more attempts than maxAttempts.
	var errs []error
	for range 10 {
		errs = append(errs, errors.New("dial tcp: network is unreachable"))
	}
	client := &fakePutClient{errs: errs}
	w := newTestWriter(t, client)
	w.maxAttempts = 3
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "echo offline\n"))
	waitForFlush(t, w)

	require.Equal(t, 11, client.calls())
	content := client.input(10).Item["content"].(*types.AttributeValueMemberS).Value
	assert.Equal(t, "echo offline\n", content)
}

func TestWriterAcceptsAnOfflineBacklogOfAnySize(t *testing.T) {
	client := &fakePutClient{offline: true}
	w := newTestWriter(t, client)
	go w.Run()
	defer w.Shutdown(context.Background())

	var want, accepted []string
	for i := range 2500 {
		line := "echo " + strconv.Itoa(i) + "\n"
		want = append(want, line)
		if enqueue(t, w, line) {
			accepted = append(accepted, line)
		}
	}
	assert.Equal(t, want, accepted)
	assert.Eventually(t, w.Stalled, 5*time.Second, time.Millisecond)

	client.setOffline(false)
	waitForFlush(t, w)

	assert.Equal(t, want, client.storedContents())
}

func TestWriterRetriesTransientErrorsIndefinitely(t *testing.T) {
	// Throttling and credential errors are transient: they must not count
	// towards the drop budget, so a line survives far more of them than
	// maxAttempts before the service finally accepts it.
	throttle := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}
	expired := &smithy.GenericAPIError{Code: "ExpiredTokenException", Message: "creds expired"}
	client := &fakePutClient{errs: []error{throttle, throttle, expired, expired, throttle, expired}}
	w := newTestWriter(t, client)
	w.maxAttempts = 2
	go w.Run()
	defer w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "echo hello\n"))
	waitForFlush(t, w)

	// Six transient failures then success: none counted towards the 2-attempt
	// drop budget.
	require.Equal(t, 7, client.calls())
}

func TestShutdownDrainsQueuedLines(t *testing.T) {
	client := &fakePutClient{}
	w := newTestWriter(t, client)
	go w.Run()

	assert.True(t, enqueue(t, w, "a\n"))
	assert.True(t, enqueue(t, w, "b\n"))
	assert.True(t, enqueue(t, w, "c\n"))

	// A clean shutdown flushes everything still queued before returning.
	remaining := w.Shutdown(context.Background())

	assert.Equal(t, int64(0), remaining)
	assert.Equal(t, 3, client.calls())
	assert.Equal(t, int64(0), w.Pending())
}

func TestLineUnflushedAtShutdownIsWrittenByTheNextWriter(t *testing.T) {
	// DynamoDB unreachable at shutdown: the drain cannot complete, so Shutdown
	// must return once its deadline passes and report the unflushed line rather
	// than block forever.
	dir := t.TempDir()
	offline := &fakePutClient{offline: true}
	w := openTestWriter(t, offline, dir)
	w.retryDelay = 10 * time.Millisecond
	go w.Run()

	assert.True(t, enqueue(t, w, "echo offline\n"))
	require.Eventually(t, func() bool { return offline.calls() > 0 }, 5*time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.Equal(t, int64(1), w.Shutdown(ctx))

	online := &fakePutClient{}
	next := openTestWriter(t, online, dir)
	go next.Run()
	defer next.Shutdown(context.Background())
	waitForFlush(t, next)

	require.Equal(t, []string{"echo offline\n"}, online.storedContents())
	assert.Equal(t, keyOf(offline.input(0)), keyOf(online.input(0)), "the replayed line must overwrite the same item")
	assert.Empty(t, journaled(t, dir))
}

func TestLineJournaledBeforeACrashIsWrittenByTheNextWriter(t *testing.T) {
	dir := t.TempDir()
	crashed := openTestWriter(t, &fakePutClient{}, dir)
	timestamp, ok := crashed.Enqueue("echo crashed\n")
	require.True(t, ok)

	client := &fakePutClient{}
	next := openTestWriter(t, client, dir)
	go next.Run()
	defer next.Shutdown(context.Background())
	waitForFlush(t, next)

	require.Equal(t, []string{"echo crashed\n"}, client.storedContents())
	assert.Equal(t, strconv.FormatInt(timestamp, 10), keyOf(client.input(0)).timestamp)
	assert.Empty(t, journaled(t, dir))
}

func TestEnqueueAfterShutdownIsWrittenByTheNextWriter(t *testing.T) {
	dir := t.TempDir()
	w := openTestWriter(t, &fakePutClient{}, dir)
	go w.Run()
	w.Shutdown(context.Background())

	assert.True(t, enqueue(t, w, "late\n"))

	client := &fakePutClient{}
	next := openTestWriter(t, client, dir)
	go next.Run()
	defer next.Shutdown(context.Background())
	waitForFlush(t, next)

	assert.Equal(t, []string{"late\n"}, client.storedContents())
}

func TestEnqueueWithoutAJournalIsRefusedOnlyAfterShutdown(t *testing.T) {
	dir := t.TempDir()
	client := &fakePutClient{}
	w := openTestWriter(t, client, dir)
	go w.Run()
	require.NoError(t, os.RemoveAll(dir))

	assert.True(t, enqueue(t, w, "kept in memory\n"))
	waitForFlush(t, w)
	assert.Equal(t, []string{"kept in memory\n"}, client.storedContents())

	w.Shutdown(context.Background())
	assert.False(t, enqueue(t, w, "late\n"))
}
