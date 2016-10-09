package blob_test

import (
	"bytes"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

type mockBlockPutter struct {
	mu      sync.Mutex
	results []putResult
}

var _ blob.BlockPutter = &mockBlockPutter{}

func (bp *mockBlockPutter) PutBlock(data []byte, bm *blob.BlockMeta) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	result := putResult{
		data: data,
		bm:   bm,
	}
	bp.results = append(bp.results, result)
	return nil
}

type putResult struct {
	data []byte
	bm   *blob.BlockMeta
}

func mustEngine(cfg blob.EngineConfig) *blob.Engine {
	e, err := blob.NewEngine(cfg)
	if err != nil {
		panic(err)
	}

	return e
}

func TestEngine_PutBlock(t *testing.T) {
	bp := &mockBlockPutter{}
	r := strings.NewReader("\x86\x4F\xD2\x6F")
	bm := (&blob.FileMeta{Size: 40, BlockSize: 4}).NewBlockMeta(5)
	e := mustEngine(blob.EngineConfig{
		UploadReaders: 1, UploadWriters: 1,
	})

	if err := e.PutBlock(r, bm, bp); err != nil {
		t.Fatalf("exp no err, got %s", err.Error())
	}

	exp := []putResult{
		{data: []byte("\x86\x4F\xD2\x6F"), bm: bm},
	}
	if !reflect.DeepEqual(bp.results, exp) {
		t.Fatalf("results: got %#v, exp %#v", bp.results, exp)
	}
}

func TestEngine_PutFile(t *testing.T) {
	e := mustEngine(blob.EngineConfig{
		UploadReaders: 1, UploadWriters: 1,
	})

	bp := &mockBlockPutter{}
	f := bytes.NewReader([]byte("\x86\x4F\xD2\x6F\xB5\x59\xF7\x5B"))
	fm, err := blob.NewFileMeta(f)
	if err != nil {
		t.Fatalf("exp no err, got %s", err.Error())
	}
	fm.Path = "/my/file"
	fm.BlockSize = 4

	progress, err := e.PutFile(f, fm, bp)
	if err != nil {
		t.Fatalf("exp no err, got %s", err.Error())
	}

	done := make(chan struct{})

	go func() {
		progress.Wait()
		close(done)
	}()

	select {
	case <-time.After(time.Second):
		t.Fatalf("did not put file in time")
	case <-done:
	}

	if len(bp.results) != 2 {
		t.Fatalf("exp 2 results, got %d", len(bp.results))
	}
}
