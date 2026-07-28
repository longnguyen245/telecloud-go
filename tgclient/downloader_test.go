package tgclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

const testChunkSize = int64(1024 * 1024)

// newTestReader builds a reader backed by deterministic in-memory content instead
// of Telegram, and records how many fetches happen per offset. fetchDelay
// simulates network latency so that prefetches are still in flight when the
// synchronous path reaches them.
func newTestReader(t *testing.T, content []byte, depth int, fetchDelay time.Duration) (*tgFileReader, *fetchRecorder) {
	t.Helper()

	// Isolate from other tests: these are package-level singletons.
	globalChunkCache = NewBlockCache(128)
	globalPrefetchSem = make(chan struct{}, 32)

	// No Telegram sessions exist in tests; keep prefetch off the real bot pool.
	prev := pickDownloadAPI
	pickDownloadAPI = func() *tg.Client { return nil }
	t.Cleanup(func() { pickDownloadAPI = prev })

	msgID := int(nextTestMsgID.Add(1))
	rec := &fetchRecorder{perOffset: map[int64]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	r := &tgFileReader{
		ctx:              ctx,
		cancel:           cancel,
		size:             int64(len(content)),
		msgID:            msgID,
		prefetchChunks:   make(map[int64][]byte),
		prefetchInflight: make(map[int64]chan struct{}),
		prefetchDepth:    depth,
		prefetchSem:      make(chan struct{}, depth),
	}
	r.fetch = func(_ *tg.Client, offset int64, limit int64) ([]byte, error) {
		rec.record(offset)
		if fetchDelay > 0 {
			time.Sleep(fetchDelay)
		}
		end := offset + limit
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		if offset >= end {
			return nil, fmt.Errorf("offset %d out of range", offset)
		}
		return append([]byte(nil), content[offset:end]...), nil
	}

	// Registered last so it runs first: prefetch goroutines must be done before
	// the cleanups above restore the package-level hooks they read.
	t.Cleanup(r.waitForPrefetches)
	return r, rec
}

var nextTestMsgID atomic.Int64

type fetchRecorder struct {
	mu        sync.Mutex
	perOffset map[int64]int
	total     int
}

func (f *fetchRecorder) record(offset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.perOffset[offset]++
	f.total++
}

func (f *fetchRecorder) snapshot() (map[int64]int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[int64]int, len(f.perOffset))
	for k, v := range f.perOffset {
		out[k] = v
	}
	return out, f.total
}

func makeContent(n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// The whole point of streaming is serving correct bytes; prefetching must not
// corrupt or reorder them.
func TestReaderStreamsExactContent(t *testing.T) {
	content := makeContent(testChunkSize*5 + 12345)
	r, _ := newTestReader(t, content, 4, 0)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

// Reads after a seek must serve data from the new position, including seeks that
// land mid-chunk (the common case for HTTP Range requests).
func TestReaderSeekServesCorrectBytes(t *testing.T) {
	content := makeContent(testChunkSize*4 + 777)
	r, _ := newTestReader(t, content, 3, 0)

	for _, off := range []int64{0, 100, testChunkSize - 1, testChunkSize, testChunkSize*2 + 5000, int64(len(content)) - 10} {
		if _, err := r.Seek(off, io.SeekStart); err != nil {
			t.Fatalf("Seek(%d): %v", off, err)
		}
		buf := make([]byte, 4096)
		n, err := io.ReadFull(io.LimitReader(r, int64(len(buf))), buf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			t.Fatalf("read at %d: %v", off, err)
		}
		want := content[off:min64(off+int64(n), int64(len(content)))]
		if !bytes.Equal(buf[:n], want) {
			t.Fatalf("mismatch after seek to %d", off)
		}
	}
}

// Regression guard for the read-ahead optimization: sequential streaming must
// actually issue concurrent fetches rather than one blocking fetch per chunk.
func TestReaderPrefetchesAheadOfCursor(t *testing.T) {
	content := makeContent(testChunkSize * 10)
	r, rec := newTestReader(t, content, 4, 0)

	// Read just the first byte, then drain the in-flight prefetches.
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}
	r.waitForPrefetches()

	offsets, _ := rec.snapshot()
	for i := int64(1); i <= 4; i++ {
		if offsets[i*testChunkSize] == 0 {
			t.Fatalf("chunk at offset %d was not prefetched; fetched offsets: %v", i*testChunkSize, offsets)
		}
	}
	if offsets[5*testChunkSize] != 0 {
		t.Fatalf("prefetched beyond the configured depth of 4")
	}
}

// A chunk already being prefetched must not be fetched a second time by the
// synchronous path, otherwise every chunk costs double the Telegram quota.
// The fetch delay makes the reader catch up with prefetches that are still in
// flight, which is exactly the case the in-flight dedup has to handle.
func TestReaderDoesNotFetchSameChunkTwice(t *testing.T) {
	content := makeContent(testChunkSize * 8)
	r, rec := newTestReader(t, content, 4, 20*time.Millisecond)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch")
	}
	r.waitForPrefetches()

	offsets, total := rec.snapshot()
	for off, count := range offsets {
		if count != 1 {
			t.Fatalf("chunk at offset %d fetched %d times, want exactly 1", off, count)
		}
	}
	if len(offsets) != 8 || total != 8 {
		t.Fatalf("fetched %d distinct chunks (%d total), want 8", len(offsets), total)
	}
}

// Seeking away must not leave results of superseded prefetches piling up in the
// per-reader buffer, which would grow memory without bound during heavy seeking.
func TestReaderDiscardsStalePrefetchAfterSeek(t *testing.T) {
	content := makeContent(testChunkSize * 20)
	r, _ := newTestReader(t, content, 4, 20*time.Millisecond)

	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if _, err := r.Seek(15*testChunkSize, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	r.waitForPrefetches()

	r.prefetchMu.Lock()
	buffered := len(r.prefetchChunks)
	r.prefetchMu.Unlock()
	if buffered != 0 {
		t.Fatalf("prefetch buffer holds %d stale chunks after seek, want 0", buffered)
	}
}

// The server-wide read-ahead budget is best-effort: when it is exhausted (many
// concurrent viewers) prefetching must simply stop rather than block, leak the
// per-reader slot, or serve wrong bytes.
func TestReaderStreamsCorrectlyWhenGlobalBudgetExhausted(t *testing.T) {
	content := makeContent(testChunkSize*4 + 999)
	r, rec := newTestReader(t, content, 4, 0)

	// Take the entire global budget so no prefetch can start.
	for i := 0; i < cap(globalPrefetchSem); i++ {
		globalPrefetchSem <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(globalPrefetchSem); i++ {
			<-globalPrefetchSem
		}
	})

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch with prefetch disabled")
	}

	// Per-reader slots must all have been returned, not leaked by the rejected path.
	if len(r.prefetchSem) != 0 {
		t.Fatalf("leaked %d per-reader prefetch slots", len(r.prefetchSem))
	}
	_, total := rec.snapshot()
	if total != 5 {
		t.Fatalf("fetched %d chunks, want 5 (synchronous only, no prefetch)", total)
	}
}

// Closing the reader mid-stream (client aborts the request, very common with
// video players) must not leave Read blocked forever waiting on a prefetch.
func TestReaderReadUnblocksOnClose(t *testing.T) {
	content := makeContent(testChunkSize * 6)
	r, _ := newTestReader(t, content, 4, 50*time.Millisecond)

	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		// Jump to a chunk that a prefetch goroutine is still working on.
		if _, err := r.Seek(2*testChunkSize, io.SeekStart); err != nil {
			done <- err
			return
		}
		big := make([]byte, 4096)
		_, err := r.Read(big)
		done <- err
	}()

	r.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not return after Close")
	}
}

// waitForPrefetches blocks until every in-flight prefetch goroutine has finished.
func (r *tgFileReader) waitForPrefetches() {
	for i := 0; i < cap(r.prefetchSem); i++ {
		r.prefetchSem <- struct{}{}
	}
	for i := 0; i < cap(r.prefetchSem); i++ {
		<-r.prefetchSem
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
