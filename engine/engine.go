package engine

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

const (
	defaultUploaders   = 10
	defaultDownloaders = 25
)

type Engine struct {
	uploaders, downloaders int

	uploads   chan uploadTask
	downloads chan downloadTask
}

func NewEngine(uploaders, downloaders int) *Engine {
	if uploaders == 0 {
		uploaders = defaultUploaders
	}
	if downloaders == 0 {
		downloaders = defaultDownloaders
	}

	e := &Engine{
		uploaders:   uploaders,
		downloaders: downloaders,

		uploads:   make(chan uploadTask, uploaders),
		downloads: make(chan downloadTask, downloaders),
	}

	for i := 0; i < uploaders; i++ {
		go e.handleUploads()
	}
	for i := 0; i < downloaders; i++ {
		go e.handleDownloads()
	}

	return e
}

func (e *Engine) NumWorkers() (uploaders, downloaders int) {
	return e.uploaders, e.downloaders
}

type BlockUploader interface {
	UploadBlock(data []byte, bm *blob.BlockMeta) error
}

// UploadFile reads from f via fm and uploads through bu.
func (e *Engine) UploadFile(f io.ReaderAt, fm *blob.FileMeta, bu BlockUploader) *FileTransferContext {
	nBlocks := fm.NumBlocks()
	ctx := &FileTransferContext{
		Blocks: make([]*BlockTransferContext, nBlocks),
		fm:     fm,
	}

	for i := 0; i < nBlocks; i++ {
		ctx.Blocks[i] = &BlockTransferContext{
			bm:   fm.NewBlockMeta(i),
			done: make(chan struct{}),
		}
	}

	go func() {
		for i := 0; i < nBlocks; i++ {
			e.uploads <- uploadTask{ctx: ctx.Blocks[i], r: f, bu: bu}
		}
	}()

	return ctx
}

func (e *Engine) handleUploads() {
	for task := range e.uploads {
		e.handleUpload(task)
	}
}

type uploadTask struct {
	ctx *BlockTransferContext
	r   io.ReaderAt
	bu  BlockUploader
}

func (e *Engine) handleUpload(t uploadTask) {
	defer close(t.ctx.done)

	t.ctx.startedAt = time.Now()
	defer func() { t.ctx.finishedAt = time.Now() }()

	bm := t.ctx.bm
	if err := bm.SetSHA256(
		io.NewSectionReader(t.r, bm.FileOffset(), int64(bm.ExpSize())),
	); err != nil {
		panic(err)
	}
	if err := UploadBlock(t.r, bm, t.bu); err != nil {
		panic(err)
	}
}

type BlockDownloader interface {
	DownloadBlock(bm *blob.BlockMeta) ([]byte, error)
}

type downloadTask struct {
	ctx *BlockTransferContext
	w   io.WriterAt
	bd  BlockDownloader
}

func (e *Engine) handleDownloads() {
	for task := range e.downloads {
		e.handleDownload(task)
	}
}

// DownloadFile attempts to download bms through bd, writing each block to w.
func (e *Engine) DownloadFile(w io.WriterAt, bms []*blob.BlockMeta, bd BlockDownloader) (*FileTransferContext, error) {
	if len(bms) == 0 {
		return nil, fmt.Errorf("(%T).DownloadFile: must have at least one BlockMeta", e)
	}

	fm := bms[0].FileMeta
	ctx := &FileTransferContext{
		Blocks: make([]*BlockTransferContext, len(bms)),
		fm:     fm,
	}

	for i, bm := range bms {
		if bm.FileMeta != fm {
			return nil, fmt.Errorf("(%T).DownloadFile: all BlockMeta must have same FileMeta", e)
		}
		ctx.Blocks[i] = &BlockTransferContext{
			bm:   bm,
			done: make(chan struct{}),
		}
	}

	go func() {
		for _, ctx := range ctx.Blocks {
			e.downloads <- downloadTask{ctx: ctx, w: w, bd: bd}
		}
	}()

	return ctx, nil
}

func (e *Engine) handleDownload(t downloadTask) {
	defer close(t.ctx.done)

	t.ctx.startedAt = time.Now()
	defer func() { t.ctx.finishedAt = time.Now() }()

	if err := DownloadBlock(t.w, t.ctx.bm, t.bd); err != nil {
		panic(err)
	}
}

// UploadBlock copies the data described by bm, from r, to bu.
// UploadBlock is safe for concurrent use.
func UploadBlock(r io.ReaderAt, bm *blob.BlockMeta, bu BlockUploader) error {
	data := make([]byte, bm.ExpSize())
	if n, err := r.ReadAt(data, bm.FileOffset()); err != nil {
		return err
	} else if n != bm.ExpSize() {
		return fmt.Errorf("did not read enough data: exp %d, got %d", bm.ExpSize(), n)
	}

	bm.SetSHA256(bytes.NewReader(data))
	return bu.UploadBlock(data, bm)
}

// DownloadBlock copies the data described by bm, from bd, into w.
// DownloadBlock is safe for concurrent use.
func DownloadBlock(w io.WriterAt, bm *blob.BlockMeta, bd BlockDownloader) error {
	data, err := bd.DownloadBlock(bm)
	if err != nil {
		return err
	}
	if len(data) != bm.ExpSize() {
		return fmt.Errorf("data did not match block size")
	}

	if n, err := w.WriteAt(data, bm.FileOffset()); err != nil {
		return err
	} else if n != bm.ExpSize() {
		return fmt.Errorf("block %d did not write expected size %d, got %d", bm.Index, bm.ExpSize(), n)
	}

	// Calculate the checksum on the data we've received,
	// and make sure it matches expected.
	if err := bm.CompareSHA256Against(bytes.NewReader(data)); err != nil {
		return err
	}

	return nil
}
