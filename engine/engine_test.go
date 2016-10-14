package engine_test

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
	"github.com/mark-rushakoff/influx-blob/engine"
)

type mockUploader struct {
	mu      sync.Mutex
	results []putResult
}

var _ engine.BlockUploader = &mockUploader{}

func (u *mockUploader) UploadBlock(data []byte, bm *blob.BlockMeta) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	result := putResult{
		data: data,
		bm:   bm,
	}
	u.results = append(u.results, result)
	return nil
}

type putResult struct {
	data []byte
	bm   *blob.BlockMeta
}

func TestEngine_UploadFile(t *testing.T) {
	e := engine.NewEngine(1, 1)

	f := strings.NewReader("abcdefgh")
	fm, err := blob.NewFileMeta(f)
	if err != nil {
		t.Fatalf("exp no err, got %s", err.Error())
	}
	fm.Path = "/my/file"
	fm.BlockSize = 4

	bu := &mockUploader{}
	ctx := e.UploadFile(f, fm, bu)

	done := make(chan struct{})
	go func() {
		ctx.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Great, done.
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("UploadFile did not complete in time")
	}

	if len(ctx.Blocks) != 2 {
		t.Fatalf("exp 2 blocks, got %d", len(ctx.Blocks))
	}
	if !ctx.Blocks[0].Done() || !ctx.Blocks[1].Done() {
		t.Fatalf("All blocks should have been marked done")
	}

	// The file's blocks will be written sequentially since we're using 1 uploader.
	if !bytes.Equal(bu.results[0].data, []byte("abcd")) ||
		!bytes.Equal(bu.results[1].data, []byte("efgh")) {
		t.Fatalf("Wrong block uploaded")
	}
}

type mockBlockDownloader struct {
	src []byte
}

var _ engine.BlockDownloader = &mockBlockDownloader{}

func (d *mockBlockDownloader) DownloadBlock(bm *blob.BlockMeta) ([]byte, error) {
	o := int(bm.FileOffset())
	data := d.src[o : o+bm.ExpSize()]

	// Copy data's checksum to bm.
	// (Normally this would have been set before calling DownloadBlock.)
	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return nil, err
	}
	copy(bm.SHA256[:], h.Sum(nil))

	return data, nil
}

type writerAt struct {
	mu  sync.Mutex
	buf []byte
}

func (w *writerAt) WriteAt(p []byte, off int64) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	o := int(off)

	if end := o + len(p); end > len(w.buf) {
		w.buf = append(w.buf, make([]byte, end-len(w.buf))...)
	}

	copy(w.buf[o:o+len(p)], p)
	return len(p), nil
}

func TestEngine_DownloadFile(t *testing.T) {
	e := engine.NewEngine(1, 1)

	// File on remote
	fm, err := blob.NewFileMeta(strings.NewReader("abcdefgh"))
	if err != nil {
		t.Fatalf("Could not create file meta: %s", err)
	}
	fm.Path = "/my/file"
	fm.BlockSize = 4
	bd := &mockBlockDownloader{
		src: []byte("abcdefgh"),
	}

	bms := make([]*blob.BlockMeta, fm.NumBlocks())
	for i := range bms {
		bms[i] = fm.NewBlockMeta(i)
	}

	w := &writerAt{}
	ctx, err := e.DownloadFile(w, bms, bd)
	if err != nil {
		t.Fatalf("Failed to download file: %s", err)
	}

	done := make(chan struct{})
	go func() {
		ctx.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Fatalf("DownloadFile did not complete in time")
	}

	if len(ctx.Blocks) != 2 {
		t.Fatalf("exp 2 blocks, got %d", len(ctx.Blocks))
	}
	if !ctx.Blocks[0].Done() || !ctx.Blocks[1].Done() {
		t.Fatalf("All blocks should have been marked done")
	}

	// The file's blocks will be written sequentially since we're using 1 uploader.
	if !bytes.Equal(w.buf, []byte("abcdefgh")) {
		t.Fatalf("Wrong blocks downloaded")
	}
}
