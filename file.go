package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
)

// ContentGetter loads the full history file content.
type ContentGetter interface {
	Get(ctx context.Context) (string, error)
}

// File implements both Node and Handle for the history file.
type File struct {
	repo   ContentGetter
	writer *HistoryWriter
	uid    uint32
	gid    uint32

	// cacheTTL is how long loaded content is served before a background
	// refresh picks up history written by other machines. <= 0 disables
	// refreshing; content is then loaded only once.
	cacheTTL time.Duration
	// loadRetryInterval throttles synchronous load attempts after a failure
	// so an unreachable DynamoDB does not stall the shell on every prompt.
	loadRetryInterval time.Duration

	mu              sync.Mutex
	data            []byte
	loaded          bool
	loading         bool
	loadedAt        time.Time
	lastLoadAttempt time.Time
	refreshing      bool
	// writeSeq increments on every local write; a load or refresh that was
	// in flight when a write happened is discarded so it cannot hide that
	// write from the local view.
	writeSeq uint64
}

func NewFile(repo ContentGetter, writer *HistoryWriter, cacheTTL time.Duration) *File {
	return &File{
		repo:              repo,
		writer:            writer,
		uid:               uint32(os.Getuid()),
		gid:               uint32(os.Getgid()),
		cacheTTL:          cacheTTL,
		loadRetryInterval: 15 * time.Second,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.ensureLoaded(ctx)

	f.mu.Lock()
	size := len(f.data)
	f.mu.Unlock()

	a.Inode = 2
	a.Mode = 0o444
	a.Size = uint64(size)
	a.Uid = f.uid
	a.Gid = f.gid
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	// Keep the page cache between opens so each new shell does not re-read
	// the entire history through FUSE; the kernel revalidates with Getattr
	// when the size changes. The cache may briefly serve a stale view after
	// a background refresh, which is acceptable for append-mostly history.
	resp.Flags |= fuse.OpenKeepCache
	return f, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	f.ensureLoaded(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	// HandleRead copies into resp.Data, so the response cannot be mutated
	// by a concurrent Write once the lock is released.
	fuseutil.HandleRead(req, resp, f.data)
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	defer duration(track("Write()"))

	f.mu.Lock()
	defer f.mu.Unlock()
	f.writer.Enqueue(string(req.Data))
	f.writeSeq++
	HandleWrite(req, resp, &f.data)
	return nil
}

// ensureLoaded loads content synchronously on first access and kicks off a
// background refresh once loaded content is older than cacheTTL.
func (f *File) ensureLoaded(ctx context.Context) {
	f.mu.Lock()
	if f.loaded {
		f.maybeRefreshLocked()
		f.mu.Unlock()
		return
	}
	if f.loading || time.Since(f.lastLoadAttempt) < f.loadRetryInterval {
		f.mu.Unlock()
		return
	}
	if f.writer.Pending() > 0 {
		// Local lines are still being flushed to DynamoDB; a scan now would
		// not include them, so wait for the queue to drain.
		f.mu.Unlock()
		return
	}
	f.loading = true
	f.lastLoadAttempt = time.Now()
	seq := f.writeSeq
	f.mu.Unlock()

	c, err := f.repo.Get(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.loading = false
	if err != nil {
		// Keep whatever is buffered locally; a failed load must not wipe
		// history that has already been written this session.
		log.Printf("WARN: could not load history content: %v", err)
		return
	}
	if f.writeSeq != seq {
		return
	}
	f.data = []byte(c)
	f.loaded = true
	f.loadedAt = time.Now()
}

// maybeRefreshLocked starts a background reload of the content when the
// cache has expired. Callers must hold f.mu. The refresh is asynchronous so
// a slow table scan never blocks the shell.
func (f *File) maybeRefreshLocked() {
	if f.cacheTTL <= 0 || f.refreshing || time.Since(f.loadedAt) < f.cacheTTL {
		return
	}
	if f.writer.Pending() > 0 {
		// Local lines are still being flushed to DynamoDB; a scan now would
		// not include them, so wait for the queue to drain.
		return
	}
	f.refreshing = true
	seq := f.writeSeq

	go func() {
		c, err := f.repo.Get(context.Background())

		f.mu.Lock()
		defer f.mu.Unlock()
		f.refreshing = false
		if err != nil {
			log.Printf("WARN: could not refresh history content: %v", err)
			// Back off for a full TTL before retrying so an outage does not
			// cause a hot refresh loop.
			f.loadedAt = time.Now()
			return
		}
		if f.writeSeq != seq {
			return
		}
		f.data = []byte(c)
		f.loadedAt = time.Now()
	}()
}

func HandleWrite(req *fuse.WriteRequest, resp *fuse.WriteResponse, data *[]byte) {
	size := len(req.Data)

	if int(req.Offset)+size > len(*data) {
		newData := make([]byte, int(req.Offset)+size)
		copy(newData, *data)
		*data = newData
	}
	n := copy((*data)[req.Offset:int(req.Offset)+size], req.Data)
	resp.Size = n
}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	log.Printf("DEBUG: %v: %v\n", msg, time.Since(start))
}
