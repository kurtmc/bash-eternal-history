package main

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRepo struct {
	mu         sync.Mutex
	content    string
	entries    []historyEntry
	err        error
	calls      int
	gate       chan struct{}
	lastCtxErr error
}

func (r *fakeRepo) Get(ctx context.Context) ([]historyEntry, error) {
	r.mu.Lock()
	r.calls++
	gate := r.gate
	r.mu.Unlock()
	if gate != nil {
		<-gate
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCtxErr = ctx.Err()
	if r.err != nil {
		return nil, r.err
	}
	if r.entries != nil {
		return r.entries, nil
	}
	return entriesOf(r.content), nil
}

func entriesOf(content string) []historyEntry {
	if content == "" {
		return nil
	}
	var entries []historyEntry
	for i, line := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		entries = append(entries, historyEntry{timestamp: int64(i + 1), content: line})
	}
	return entries
}

func (r *fakeRepo) ctxErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastCtxErr
}

func (r *fakeRepo) setContent(content string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.content = content
}

func (r *fakeRepo) setEntries(entries []historyEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = entries
}

func (r *fakeRepo) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *fakeRepo) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func newTestFile(t *testing.T, repo ContentGetter) *File {
	t.Helper()
	w := newTestWriter(&fakePutClient{})
	go w.Run()
	t.Cleanup(func() { w.Shutdown(context.Background()) })

	f := NewFile(repo, w, 0, time.Minute)
	f.loadRetryInterval = 0
	return f
}

// readFile reads like the FUSE serve loop does: resp.Data is pre-allocated
// with the request size as its capacity.
func readFile(t *testing.T, f *File, offset int64, size int) string {
	t.Helper()
	return readFileCtx(t, context.Background(), f, offset, size)
}

func readFileCtx(t *testing.T, ctx context.Context, f *File, offset int64, size int) string {
	t.Helper()
	resp := &fuse.ReadResponse{Data: make([]byte, 0, size)}
	require.NoError(t, f.Read(ctx, &fuse.ReadRequest{Offset: offset, Size: size}, resp))
	return string(resp.Data)
}

func readAll(t *testing.T, f *File) string {
	t.Helper()
	return readFile(t, f, 0, 1<<20)
}

type readResult struct {
	data string
	err  error
}

func readAllAsync(ctx context.Context, f *File) <-chan readResult {
	result := make(chan readResult, 1)
	go func() {
		resp := &fuse.ReadResponse{Data: make([]byte, 0, 1<<20)}
		err := f.Read(ctx, &fuse.ReadRequest{Offset: 0, Size: 1 << 20}, resp)
		result <- readResult{data: string(resp.Data), err: err}
	}()
	return result
}

func awaitRead(t *testing.T, result <-chan readResult) string {
	t.Helper()
	r := <-result
	require.NoError(t, r.err)
	return r.data
}

func doWrite(t *testing.T, f *File, offset int64, data string) {
	t.Helper()
	resp := &fuse.WriteResponse{}
	require.NoError(t, f.Write(context.Background(), &fuse.WriteRequest{Offset: offset, Data: []byte(data)}, resp))
	require.Equal(t, len(data), resp.Size)
}

func currentData(f *File) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.data)
}

func loadInFlight(f *File) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inflight != nil
}

func waitForLoadCount(t *testing.T, repo *fakeRepo, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return repo.callCount() == n }, time.Second, time.Millisecond)
}

func TestFileLoadsContentOnce(t *testing.T) {
	repo := &fakeRepo{content: "abc\n"}
	f := newTestFile(t, repo)

	assert.Equal(t, "abc\n", readAll(t, f))
	assert.Equal(t, "abc\n", readAll(t, f))
	assert.Equal(t, 1, repo.callCount())
}

func TestOpenKeepsPageCache(t *testing.T) {
	repo := &fakeRepo{content: "abc\n"}
	f := newTestFile(t, repo)

	resp := &fuse.OpenResponse{}
	handle, err := f.Open(context.Background(), &fuse.OpenRequest{}, resp)

	require.NoError(t, err)
	assert.Same(t, f, handle)
	assert.NotZero(t, resp.Flags&fuse.OpenKeepCache)
}

func TestFileAttr(t *testing.T) {
	repo := &fakeRepo{content: "abc\n"}
	f := newTestFile(t, repo)

	var attr fuse.Attr
	require.NoError(t, f.Attr(context.Background(), &attr))

	assert.Equal(t, uint64(2), attr.Inode)
	assert.Equal(t, uint64(4), attr.Size)
	assert.Equal(t, uint32(os.Getuid()), attr.Uid)
	assert.Equal(t, uint32(os.Getgid()), attr.Gid)
}

func TestFailedLoadDoesNotWipeLocalWrites(t *testing.T) {
	repo := &fakeRepo{err: errors.New("dynamodb down")}
	f := newTestFile(t, repo)

	assert.Empty(t, readAll(t, f))
	doWrite(t, f, 0, "echo hi\n")
	waitForFlush(t, f.writer)

	// The next read attempts another load, which fails again; the buffered
	// write must survive it.
	assert.Equal(t, "echo hi\n", readAll(t, f))
	assert.GreaterOrEqual(t, repo.callCount(), 2)
}

func TestFailedLoadsAreThrottled(t *testing.T) {
	repo := &fakeRepo{err: errors.New("dynamodb down")}
	f := newTestFile(t, repo)
	f.loadRetryInterval = time.Hour

	readAll(t, f)
	readAll(t, f)

	assert.Equal(t, 1, repo.callCount())
}

func TestFailedLoadThrottleMeasuredFromCompletion(t *testing.T) {
	// A load that fails slowly must still leave a full loadRetryInterval before
	// the next attempt. The throttle is stamped when the attempt finishes, not
	// when it starts, so a slow failure does not immediately re-attempt.
	gate := make(chan struct{})
	repo := &fakeRepo{err: errors.New("dynamodb down"), gate: gate}
	f := newTestFile(t, repo)
	f.loadRetryInterval = 100 * time.Millisecond

	result := readAllAsync(context.Background(), f)
	waitForLoadCount(t, repo, 1)
	// Hold the failing load open well past loadRetryInterval. A throttle stamped
	// at the attempt's start would already have expired by the time it finishes.
	time.Sleep(200 * time.Millisecond)
	close(gate)
	awaitRead(t, result)

	// Stamped at completion, so an immediate retry is throttled.
	readAll(t, f)
	assert.Equal(t, 1, repo.callCount())
}

func TestReadWaitsForInitialLoad(t *testing.T) {
	// bash reads its history file once at startup and never again.
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old\n", gate: gate}
	f := newTestFile(t, repo)

	result := readAllAsync(context.Background(), f)
	waitForLoadCount(t, repo, 1)
	select {
	case r := <-result:
		t.Fatalf("read returned %q before the load finished", r.data)
	case <-time.After(50 * time.Millisecond):
	}

	close(gate)
	assert.Equal(t, "old\n", awaitRead(t, result))
}

func TestReadWaitIsBounded(t *testing.T) {
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old\n", gate: gate}
	f := newTestFile(t, repo)
	f.loadWaitTimeout = 20 * time.Millisecond

	result := readAllAsync(context.Background(), f)
	select {
	case r := <-result:
		require.NoError(t, r.err)
		assert.Empty(t, r.data)
	case <-time.After(time.Second):
		t.Fatal("read did not return after loadWaitTimeout")
	}
	assert.True(t, loadInFlight(f))

	close(gate)
	require.Eventually(t, func() bool { return readAll(t, f) == "old\n" }, time.Second, time.Millisecond)
	assert.Equal(t, 1, repo.callCount())
}

func TestRequestCancellationDoesNotCancelLoad(t *testing.T) {
	// The kernel interrupts a FUSE request when the reading process gets a signal.
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old\n", gate: gate}
	f := newTestFile(t, repo)

	ctx, cancel := context.WithCancel(context.Background())
	result := readAllAsync(ctx, f)
	waitForLoadCount(t, repo, 1)
	cancel()
	select {
	case r := <-result:
		require.NoError(t, r.err)
		assert.Empty(t, r.data)
	case <-time.After(time.Second):
		t.Fatal("cancelled read did not return")
	}
	assert.True(t, loadInFlight(f))

	close(gate)
	require.Eventually(t, func() bool { return readAll(t, f) == "old\n" }, time.Second, time.Millisecond)
	assert.Equal(t, 1, repo.callCount())
	require.NoError(t, repo.ctxErr(), "the reader's cancellation must not reach the load")
}

func TestConcurrentReadersShareOneLoad(t *testing.T) {
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old\n", gate: gate}
	f := newTestFile(t, repo)

	var results []<-chan readResult
	for range 5 {
		results = append(results, readAllAsync(context.Background(), f))
	}
	waitForLoadCount(t, repo, 1)
	time.Sleep(20 * time.Millisecond)
	close(gate)

	for _, result := range results {
		assert.Equal(t, "old\n", awaitRead(t, result))
	}
	assert.Equal(t, 1, repo.callCount())
}

func TestWarmLoadsWithoutARequest(t *testing.T) {
	repo := &fakeRepo{content: "x\n"}
	f := newTestFile(t, repo)

	f.Warm()

	assert.Equal(t, 1, repo.callCount())
	assert.Equal(t, "x\n", readAll(t, f))
	assert.Equal(t, 1, repo.callCount())
}

func TestWriteDuringLoadIsMerged(t *testing.T) {
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old1\nold2\n", gate: gate}
	f := newTestFile(t, repo)
	f.loadRetryInterval = time.Hour

	result := readAllAsync(context.Background(), f)
	waitForLoadCount(t, repo, 1)
	doWrite(t, f, 0, "echo new\n")
	close(gate)

	assert.Equal(t, "old1\nold2\necho new\n", awaitRead(t, result))
	assert.Equal(t, 1, repo.callCount())
}

func TestWriteAlreadyInScanIsNotDuplicated(t *testing.T) {
	// The write was flushed before the scan reached its key, so the scan has it.
	gate := make(chan struct{})
	repo := &fakeRepo{gate: gate}
	f := newTestFile(t, repo)
	client := f.writer.svc.(*fakePutClient)

	result := readAllAsync(context.Background(), f)
	waitForLoadCount(t, repo, 1)
	doWrite(t, f, 0, "echo new\n")
	waitForFlush(t, f.writer)

	stored, err := strconv.ParseInt(client.input(0).Item["timestamp"].(*types.AttributeValueMemberN).Value, 10, 64)
	require.NoError(t, err)
	repo.setEntries([]historyEntry{
		{timestamp: 1, content: "old1"},
		{timestamp: 2, content: "old2"},
		{timestamp: stored, content: "echo new"},
	})
	close(gate)

	assert.Equal(t, "old1\nold2\necho new\n", awaitRead(t, result))
}

func TestLoadRecoversAfterError(t *testing.T) {
	repo := &fakeRepo{err: errors.New("dynamodb down")}
	f := newTestFile(t, repo)

	assert.Empty(t, readAll(t, f))

	repo.setError(nil)
	repo.setContent("abc\n")
	assert.Equal(t, "abc\n", readAll(t, f))
}

func TestRefreshPicksUpRemoteContent(t *testing.T) {
	repo := &fakeRepo{content: "a\n"}
	f := newTestFile(t, repo)
	f.cacheTTL = time.Millisecond

	assert.Equal(t, "a\n", readAll(t, f))

	repo.setContent("a\nb\n")
	require.Eventually(t, func() bool {
		// Reads serve the cached content and trigger the background refresh
		// once the TTL has expired.
		return readAll(t, f) == "a\nb\n"
	}, 5*time.Second, 5*time.Millisecond)
}

func TestRefreshDisabledByDefault(t *testing.T) {
	repo := &fakeRepo{content: "a\n"}
	f := newTestFile(t, repo) // cacheTTL 0

	assert.Equal(t, "a\n", readAll(t, f))
	repo.setContent("a\nb\n")
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, "a\n", readAll(t, f))
	assert.Equal(t, 1, repo.callCount())
}

func TestRefreshWaitsForPendingWrites(t *testing.T) {
	repo := &fakeRepo{content: "a\n"}
	w := newTestWriter(&fakePutClient{})
	// Run() is intentionally not started, so enqueued lines stay pending.
	f := NewFile(repo, w, time.Millisecond, time.Minute)
	f.loadRetryInterval = 0

	assert.Equal(t, "a\n", readAll(t, f))
	doWrite(t, f, 2, "echo hi\n")
	require.Equal(t, int64(1), w.Pending())

	repo.setContent("a\nREMOTE\n")
	time.Sleep(10 * time.Millisecond)
	readAll(t, f)
	time.Sleep(10 * time.Millisecond)

	// No refresh may run while local lines are still unflushed.
	assert.Equal(t, 1, repo.callCount())
	assert.NotContains(t, currentData(f), "REMOTE")
}

func TestRefreshMergesWriteThatRacesIt(t *testing.T) {
	repo := &fakeRepo{content: "a\n"}
	f := newTestFile(t, repo)
	f.cacheTTL = time.Nanosecond

	assert.Equal(t, "a\n", readAll(t, f))
	require.Equal(t, 1, repo.callCount())

	// Make the next refresh block, then trigger it.
	gate := make(chan struct{})
	repo.mu.Lock()
	repo.gate = gate
	repo.content = "a\nREMOTE\n"
	repo.mu.Unlock()

	require.NoError(t, f.Attr(context.Background(), &fuse.Attr{}))
	waitForLoadCount(t, repo, 2)

	// A write lands while the refresh is in flight.
	doWrite(t, f, 2, "echo local\n")
	close(gate)

	require.Eventually(t, func() bool { return !loadInFlight(f) }, time.Second, time.Millisecond)

	assert.Equal(t, "a\nREMOTE\necho local\n", currentData(f))
}

func TestConcurrentReadsWritesAndAttrs(t *testing.T) {
	repo := &fakeRepo{content: "abcdefghij\n"}
	f := newTestFile(t, repo)
	readAll(t, f)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 200 {
				readFile(t, f, 0, 8)
			}
		})
		wg.Go(func() {
			for j := range 200 {
				doWrite(t, f, int64(j%8), "x")
				var attr fuse.Attr
				require.NoError(t, f.Attr(context.Background(), &attr))
			}
		})
	}
	wg.Wait()
}

func TestWriteAppendsRegardlessOfOffset(t *testing.T) {
	repo := &fakeRepo{content: "aaaa\n"}
	f := newTestFile(t, repo)
	require.Equal(t, "aaaa\n", readAll(t, f))

	// A write whose offset is behind the true end of the file (e.g. an
	// O_APPEND write resolved against a stale cached size after a refresh grew
	// the content) must still append, not overwrite the existing bytes.
	doWrite(t, f, 0, "bbbb\n")
	assert.Equal(t, "aaaa\nbbbb\n", currentData(f))

	// A write past the end must not zero-fill a gap either; it just appends.
	doWrite(t, f, 9999, "cccc\n")
	assert.Equal(t, "aaaa\nbbbb\ncccc\n", currentData(f))
}

func TestWriteEnqueuesEveryLine(t *testing.T) {
	repo := &fakeRepo{content: ""}
	w := newTestWriter(&fakePutClient{})
	client := w.svc.(*fakePutClient)
	go w.Run()
	t.Cleanup(func() { w.Shutdown(context.Background()) })
	f := NewFile(repo, w, 0, time.Minute)
	f.loadRetryInterval = 0
	require.Empty(t, readAll(t, f))

	doWrite(t, f, 0, "echo one\n")
	doWrite(t, f, 100, "echo two\n")
	waitForFlush(t, w)

	require.Equal(t, 2, client.calls())
	assert.Equal(t, "echo one\n", client.input(0).Item["content"].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, "echo two\n", client.input(1).Item["content"].(*types.AttributeValueMemberS).Value)
}

func TestSetattrRejectsTruncation(t *testing.T) {
	repo := &fakeRepo{content: "secret\n"}
	f := newTestFile(t, repo)
	require.Equal(t, "secret\n", readAll(t, f))

	// O_TRUNC reaches the file as a Setattr that sets the size; it must fail
	// loudly rather than silently no-op while leaving the content intact.
	resp := &fuse.SetattrResponse{}
	err := f.Setattr(context.Background(), &fuse.SetattrRequest{Valid: fuse.SetattrSize, Size: 0}, resp)
	require.ErrorIs(t, err, syscall.EPERM)
	assert.Equal(t, "secret\n", currentData(f))
}

func TestSetattrAllowsNonSizeChanges(t *testing.T) {
	repo := &fakeRepo{content: "abc\n"}
	f := newTestFile(t, repo)

	// A Setattr that does not touch the size (e.g. a chmod/utimes) is accepted
	// as a no-op and reports the current attributes.
	resp := &fuse.SetattrResponse{}
	require.NoError(t, f.Setattr(context.Background(), &fuse.SetattrRequest{Valid: fuse.SetattrMtime}, resp))
	assert.Equal(t, uint64(4), resp.Attr.Size)
}

func TestMergeLocalWritesKeepsUnstoredAndDropsStored(t *testing.T) {
	entries := []historyEntry{
		{timestamp: 1, content: "old"},
		{timestamp: 100, content: "#1\nflushed"},
		{timestamp: 150, content: "other machine"},
	}
	writes := []localWrite{
		{timestamp: 100, data: []byte("#1\nflushed\n")},
		{timestamp: 120, data: []byte("not flushed yet\n")},
		{timestamp: 150, data: []byte("same timestamp, different line\n")},
	}

	got := string(mergeLocalWrites(entries, writes))

	assert.Equal(t, "old\n#1\nflushed\nother machine\nnot flushed yet\nsame timestamp, different line\n", got)
}
