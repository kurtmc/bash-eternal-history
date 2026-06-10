package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"bazil.org/fuse"
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
}

func (r *fakeRepo) Get(ctx context.Context) (string, error) {
	r.mu.Lock()
	r.calls++
	content, err, gate := r.content, r.err, r.gate
	r.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return content, err
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
	t.Cleanup(func() { close(w.ch) })

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

	assert.Equal(t, "", readAll(t, f))
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

func TestLoadRecoversAfterError(t *testing.T) {
	repo := &fakeRepo{err: errors.New("dynamodb down")}
	f := newTestFile(t, repo)

	assert.Equal(t, "", readAll(t, f))

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

func TestHandleWrite(t *testing.T) {
	t.Run("append at end", func(t *testing.T) {
		data := []byte("abc")
		resp := &fuse.WriteResponse{}
		HandleWrite(&fuse.WriteRequest{Offset: 3, Data: []byte("def")}, resp, &data)
		assert.Equal(t, "abcdef", string(data))
		assert.Equal(t, 3, resp.Size)
	})

	t.Run("overwrite in place", func(t *testing.T) {
		data := []byte("abcdef")
		resp := &fuse.WriteResponse{}
		HandleWrite(&fuse.WriteRequest{Offset: 1, Data: []byte("ZZ")}, resp, &data)
		assert.Equal(t, "aZZdef", string(data))
		assert.Equal(t, 2, resp.Size)
	})

	t.Run("write past end zero-fills the gap", func(t *testing.T) {
		data := []byte("ab")
		resp := &fuse.WriteResponse{}
		HandleWrite(&fuse.WriteRequest{Offset: 4, Data: []byte("cd")}, resp, &data)
		assert.Equal(t, "ab\x00\x00cd", string(data))
		assert.Equal(t, 2, resp.Size)
	})

	t.Run("write to empty file", func(t *testing.T) {
		var data []byte
		resp := &fuse.WriteResponse{}
		HandleWrite(&fuse.WriteRequest{Offset: 0, Data: []byte("hi")}, resp, &data)
		assert.Equal(t, "hi", string(data))
		assert.Equal(t, 2, resp.Size)
	})
}
