package main

import (
	"context"
	"errors"
	"os"
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
	mu      sync.Mutex
	content string
	err     error
	calls   int
	// gate, when non-nil, blocks Get until it is closed.
	gate chan struct{}
	// lastHadDeadline records whether the most recent Get's context carried a
	// deadline, so a test can tell a bounded foreground load apart from an
	// unbounded background one.
	lastHadDeadline bool
}

func (r *fakeRepo) Get(ctx context.Context) (string, error) {
	r.mu.Lock()
	r.calls++
	_, r.lastHadDeadline = ctx.Deadline()
	content, err, gate := r.content, r.err, r.gate
	r.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return content, err
}

func (r *fakeRepo) hadDeadline() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastHadDeadline
}

func (r *fakeRepo) setContent(content string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.content = content
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

	f := NewFile(repo, w, 0)
	f.loadRetryInterval = 0
	return f
}

// readFile reads like the FUSE serve loop does: resp.Data is pre-allocated
// with the request size as its capacity.
func readFile(t *testing.T, f *File, offset int64, size int) string {
	t.Helper()
	resp := &fuse.ReadResponse{Data: make([]byte, 0, size)}
	require.NoError(t, f.Read(context.Background(), &fuse.ReadRequest{Offset: offset, Size: size}, resp))
	return string(resp.Data)
}

func readAll(t *testing.T, f *File) string {
	t.Helper()
	return readFile(t, f, 0, 1<<20)
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

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp := &fuse.ReadResponse{Data: make([]byte, 0, 1<<20)}
		_ = f.Read(context.Background(), &fuse.ReadRequest{Offset: 0, Size: 1 << 20}, resp)
	}()
	require.Eventually(t, func() bool { return repo.callCount() == 1 }, time.Second, time.Millisecond)
	// Hold the failing load open well past loadRetryInterval. A throttle stamped
	// at the attempt's start would already have expired by the time it finishes.
	time.Sleep(200 * time.Millisecond)
	close(gate)
	<-done

	// Stamped at completion, so an immediate retry is throttled.
	readAll(t, f)
	assert.Equal(t, 1, repo.callCount())
}

func TestForegroundLoadIsBoundedButWarmIsNot(t *testing.T) {
	// A request-path (Read/Attr) load must carry a deadline so a shell never
	// wedges on a cold scan, while the background Warm load must stay unbounded
	// so a table too large to scan within that bound still loads fully.
	repo := &fakeRepo{content: "x\n"}
	f := newTestFile(t, repo)
	f.foregroundLoadTimeout = 5 * time.Second

	readAll(t, f) // foreground load
	assert.True(t, repo.hadDeadline(), "foreground load context should carry a deadline")

	// A fresh file loaded via Warm must use an unbounded context.
	warmed := newTestFile(t, repo)
	warmed.Warm()
	assert.False(t, repo.hadDeadline(), "Warm load context should not carry a deadline")
}

func TestColdLoadRaceDoesNotHideHistory(t *testing.T) {
	// A local write that races the very first (cold) load makes the load result
	// stale, so it is discarded. The discard must NOT throttle the next load,
	// or the entire eternal history would stay hidden behind the buffered local
	// write for a full loadRetryInterval.
	gate := make(chan struct{})
	repo := &fakeRepo{content: "old1\nold2\n", gate: gate}
	f := newTestFile(t, repo)
	f.loadRetryInterval = time.Hour // a throttle here would hide history "forever"

	// Start the cold load; it blocks in Get until the gate is released.
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp := &fuse.ReadResponse{Data: make([]byte, 0, 1<<20)}
		_ = f.Read(context.Background(), &fuse.ReadRequest{Offset: 0, Size: 1 << 20}, resp)
	}()
	require.Eventually(t, func() bool { return repo.callCount() == 1 }, time.Second, time.Millisecond)

	// A write lands while the load is in flight: it bumps writeSeq so the load
	// result is discarded. The line is now visible in the table too.
	doWrite(t, f, 0, "echo new\n")
	repo.setContent("old1\nold2\necho new\n")
	close(gate)
	<-done
	waitForFlush(t, f.writer)

	// The next read must retry (not be throttled) and surface the full history.
	assert.Equal(t, "old1\nold2\necho new\n", readAll(t, f))
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
	f := NewFile(repo, w, time.Millisecond)
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

func TestRefreshDiscardedWhenWriteRacesIt(t *testing.T) {
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
	require.Eventually(t, func() bool { return repo.callCount() == 2 }, time.Second, time.Millisecond)

	// A write lands while the refresh is in flight.
	doWrite(t, f, 2, "echo local\n")
	close(gate)

	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return !f.refreshing
	}, time.Second, time.Millisecond)

	// The stale refresh result must be discarded so it cannot hide the
	// write that raced it.
	assert.Contains(t, currentData(f), "echo local\n")
	assert.NotContains(t, currentData(f), "REMOTE")
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
	f := NewFile(repo, w, 0)
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
