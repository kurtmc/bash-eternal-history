package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
)

// ContentGetter loads the full history from the table.
type ContentGetter interface {
	Get(ctx context.Context) ([]historyEntry, error)
}

type localWrite struct {
	timestamp int64
	data      []byte
}

// File implements both Node and Handle for the history file.
type File struct {
	repo   ContentGetter
	writer *HistoryWriter
	uid    uint32
	gid    uint32

	cacheTTL          time.Duration
	loadRetryInterval time.Duration
	loadWaitTimeout   time.Duration

	mu               sync.Mutex
	data             []byte
	loaded           bool
	loadedAt         time.Time
	lastLoadAttempt  time.Time
	inflight         chan struct{}
	writesDuringLoad []localWrite
}

func NewFile(repo ContentGetter, writer *HistoryWriter, cacheTTL, loadWaitTimeout time.Duration) *File {
	return &File{
		repo:              repo,
		writer:            writer,
		uid:               uint32(os.Getuid()),
		gid:               uint32(os.Getgid()),
		cacheTTL:          cacheTTL,
		loadRetryInterval: 15 * time.Second,
		loadWaitTimeout:   loadWaitTimeout,
	}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.awaitLoad(ctx)

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
	f.awaitLoad(ctx)

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
	timestamp, _ := f.writer.Enqueue(string(req.Data))
	if f.inflight != nil {
		f.writesDuringLoad = append(f.writesDuringLoad, localWrite{timestamp: timestamp, data: bytes.Clone(req.Data)})
	}
	f.data = append(f.data, req.Data...)
	resp.Size = len(req.Data)
	return nil
}

func (f *File) awaitLoad(ctx context.Context) {
	f.mu.Lock()
	if f.loaded {
		f.maybeRefreshLocked()
		f.mu.Unlock()
		return
	}
	done := f.startLoadLocked()
	f.mu.Unlock()
	if done == nil {
		return
	}

	if f.loadWaitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.loadWaitTimeout)
		defer cancel()
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (f *File) startLoadLocked() <-chan struct{} {
	if f.inflight != nil {
		return f.inflight
	}
	if time.Since(f.lastLoadAttempt) < f.loadRetryInterval {
		return nil
	}
	if f.writer.Pending() > 0 {
		return nil
	}
	done := make(chan struct{})
	f.inflight = done
	f.writesDuringLoad = nil
	go f.load(done)
	return done
}

func (f *File) load(done chan struct{}) {
	defer close(done)
	entries, err := f.repo.Get(context.Background())

	f.mu.Lock()
	defer f.mu.Unlock()
	f.inflight = nil
	f.lastLoadAttempt = time.Now()
	writes := f.writesDuringLoad
	f.writesDuringLoad = nil
	if err != nil {
		if f.loaded {
			f.loadedAt = time.Now()
			log.Printf("WARN: could not refresh history content: %v", err)
		} else {
			log.Printf("WARN: could not load history content: %v", err)
		}
		return
	}
	f.data = mergeLocalWrites(entries, writes)
	f.loaded = true
	f.loadedAt = time.Now()
}

func mergeLocalWrites(entries []historyEntry, writes []localWrite) []byte {
	data := renderEntries(entries)
	if len(writes) == 0 {
		return data
	}

	oldest := writes[0].timestamp
	for _, w := range writes {
		oldest = min(oldest, w.timestamp)
	}
	stored := make(map[int64]string)
	for i := len(entries) - 1; i >= 0 && entries[i].timestamp >= oldest; i-- {
		stored[entries[i].timestamp] = entries[i].content
	}
	for _, w := range writes {
		if content, ok := stored[w.timestamp]; ok && content == strings.TrimRight(string(w.data), "\n") {
			continue
		}
		data = append(data, w.data...)
	}
	return data
}

func (f *File) maybeRefreshLocked() {
	if f.cacheTTL <= 0 || f.inflight != nil || time.Since(f.loadedAt) < f.cacheTTL {
		return
	}
	f.startLoadLocked()
}

func (f *File) Warm() {
	retry := f.loadRetryInterval
	if retry <= 0 {
		retry = time.Second
	}
	for {
		f.mu.Lock()
		if f.loaded {
			f.mu.Unlock()
			return
		}
		done := f.startLoadLocked()
		f.mu.Unlock()
		if done != nil {
			<-done
		}

		f.mu.Lock()
		loaded := f.loaded
		f.mu.Unlock()
		if loaded {
			return
		}
		time.Sleep(retry)
	}
}
