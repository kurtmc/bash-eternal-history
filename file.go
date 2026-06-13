package main

import (
	"context"
	"log"
	"os"
	"sync"
	"syscall"
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
	// foregroundLoadTimeout bounds a load triggered from a FUSE request (Attr /
	// Read) so a shell never wedges on a cold scan of a very large table. The
	// background Warm and refresh loads are deliberately not bounded this way,
	// so the table still loads fully — just off the request path.
	foregroundLoadTimeout time.Duration

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
		repo:                  repo,
		writer:                writer,
		uid:                   uint32(os.Getuid()),
		gid:                   uint32(os.Getgid()),
		cacheTTL:              cacheTTL,
		loadRetryInterval:     15 * time.Second,
		foregroundLoadTimeout: 15 * time.Second,
	}
}

// loadForeground runs ensureLoaded for a FUSE request, bounding the load so a
// shell never wedges on a cold scan of a very large table.
func (f *File) loadForeground(ctx context.Context) {
	if f.foregroundLoadTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.foregroundLoadTimeout)
		defer cancel()
	}
	f.ensureLoaded(ctx)
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.loadForeground(ctx)

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

// Setattr handles metadata changes. The history file is an append-only log, so
// any attempt to change its size (i.e. truncate it) is refused with EPERM. The
// kernel turns an O_TRUNC open into a Setattr(size=0); without this method the
// FUSE library answers it with silent success, so a tool that "cleared" the
// file would appear to succeed while nothing was deleted and the content
// reappeared on the next refresh. Failing loudly is the honest answer.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		return syscall.EPERM
	}
	// Every other attribute (mode/uid/gid/times) is fixed; report the current
	// attributes and ignore the requested change.
	return f.Attr(ctx, &resp.Attr)
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
	f.loadForeground(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	// HandleRead copies into resp.Data, so the response cannot be mutated
	// by a concurrent Write once the lock is released.
	fuseutil.HandleRead(req, resp, f.data)
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// The history file is an append-only log, so the kernel-supplied offset is
	// deliberately ignored and every write is appended. Honoring the offset
	// would let a write land mid-file when a background refresh has grown the
	// content under a stale cached size (corrupting the local view), and would
	// let an arbitrary pwrite offset balloon the buffer or crash the daemon.
	// Appending is exactly what bash's `history -a` wants.
	f.writer.Enqueue(string(req.Data))
	f.writeSeq++
	f.data = append(f.data, req.Data...)
	resp.Size = len(req.Data)
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
	seq := f.writeSeq
	f.mu.Unlock()

	c, err := f.repo.Get(ctx)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.loading = false
	if err != nil {
		// Stamp the throttle once the attempt has finished, not when it
		// started: a slow failure (e.g. a black-holed network taking the full
		// read timeout) must still leave a full loadRetryInterval before the
		// next attempt, otherwise the very next FUSE callback would immediately
		// start another blocking load. Keep whatever is buffered locally; a
		// failed load must not wipe history already written this session.
		f.lastLoadAttempt = time.Now()
		log.Printf("WARN: could not load history content: %v", err)
		return
	}
	if f.writeSeq != seq {
		// A local write raced this first load, so the scan result is missing
		// it. Discard the result, but do NOT stamp the throttle: the buffered
		// local write is currently the only visible content, so the next FUSE
		// callback must be allowed to retry and load the rest of the history
		// rather than serve a near-empty file for a whole loadRetryInterval.
		return
	}
	f.lastLoadAttempt = time.Now()
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
			// A local write raced this refresh; discard the stale result, but
			// reset the clock anyway. Otherwise loadedAt stays expired and a
			// steady stream of local writes would make every following FUSE
			// callback restart a full table scan (read and allocation churn).
			f.loadedAt = time.Now()
			return
		}
		f.data = []byte(c)
		f.loadedAt = time.Now()
	}()
}

// Warm loads the history content in the background, off the FUSE request path,
// and keeps retrying until it succeeds. The request-path load is bounded by
// foregroundLoadTimeout so a shell never wedges on a cold scan; this loader is
// deliberately unbounded so a table too large to scan within that bound still
// loads fully here rather than failing forever on the request path, and so a
// load that failed because the machine was offline at boot is retried once
// connectivity returns.
func (f *File) Warm() {
	for {
		f.ensureLoaded(context.Background())

		f.mu.Lock()
		loaded := f.loaded
		f.mu.Unlock()
		if loaded {
			return
		}

		retry := f.loadRetryInterval
		if retry <= 0 {
			retry = time.Second
		}
		time.Sleep(retry)
	}
}
