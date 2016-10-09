package blob

import (
	"bytes"
	"fmt"
	"io"
	"sync"
)

const (
	defaultFileReaders = 10
	defaultFileWriters = 25
)

type BlockPutter interface {
	PutBlock(data []byte, bm *BlockMeta) error
}

type EngineConfig struct {
	UploadReaders, UploadWriters     int
	DownloadReaders, DownloadWriters int
}

type Engine struct {
	cfg EngineConfig

	upR, upW     chan struct{}
	downR, downW chan struct{}
}

func NewEngine(cfg EngineConfig) (*Engine, error) {
	e := &Engine{
		cfg: cfg,

		// Semaphores
		upR:   make(chan struct{}, cfg.UploadReaders),
		upW:   make(chan struct{}, cfg.UploadWriters),
		downR: make(chan struct{}, cfg.DownloadReaders),
		downW: make(chan struct{}, cfg.DownloadWriters),
	}
	// Fill semaphores
	go func() {
		for i := 0; i < cfg.UploadReaders; i++ {
			e.upR <- struct{}{}
		}
		for i := 0; i < cfg.UploadWriters; i++ {
			e.upW <- struct{}{}
		}
		for i := 0; i < cfg.DownloadReaders; i++ {
			e.downR <- struct{}{}
		}
		for i := 0; i < cfg.DownloadWriters; i++ {
			e.downW <- struct{}{}
		}
	}()
	return e, nil
}

// PutFile stores the data in f, described by m, into v as a new file.
// It returns a PutFileProgress that can be interrogated
func (e *Engine) PutFile(f io.ReaderAt, fm *FileMeta, bp BlockPutter) (*PutFileProgress, error) {
	var wg sync.WaitGroup
	nBlocks := fm.NumBlocks()
	for i := 0; i < nBlocks; i++ {
		wg.Add(1)
		bm := fm.NewBlockMeta(i)
		go func() {
			defer wg.Done()
			// Acquire/release semaphore.
			<-e.upR
			defer func() { e.upR <- struct{}{} }()

			r := io.NewSectionReader(f, int64(bm.offset), int64(bm.expSize))
			if err := e.PutBlock(r, bm, bp); err != nil {
				panic(err)
			}
			// TODO: notify somehow of completion?
		}()
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	return &PutFileProgress{done: done}, nil
}

func (e *Engine) PutBlock(r io.Reader, bm *BlockMeta, bp BlockPutter) error {
	data := make([]byte, 0, bm.expSize)
	buf := bytes.NewBuffer(data)
	if _, err := io.Copy(buf, r); err != nil {
		return err
	}
	bm.SetSHA256(bytes.NewReader(data))
	return bp.PutBlock(data, bm)
}

type PutFileProgress struct {
	done <-chan struct{}
}

// Wait blocks until the PutFile operation is complete.
func (p *PutFileProgress) Wait() {
	// The read will return immediately if the channel is closed.
	<-p.done
}

func (e *Engine) GetFile(w io.WriterAt, fm *FileMeta, bg BlockGetter) (*PutFileProgress, error) {
	var wg sync.WaitGroup
	nBlocks := fm.NumBlocks()
	for i := 0; i < nBlocks; i++ {
		wg.Add(1)
		bm := fm.NewBlockMeta(i)
		go func() {
			defer wg.Done()
			// Acquire/release semaphore.
			<-e.upR
			defer func() { e.upR <- struct{}{} }()
			if err := e.GetBlock(w, bm, bg); err != nil {
				panic(err)
			}
		}()
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	return &PutFileProgress{done: done}, nil
}

type BlockGetter interface {
	GetBlock(*BlockMeta) ([]byte, error)
}

func (e *Engine) GetBlock(w io.WriterAt, bm *BlockMeta, bg BlockGetter) error {
	data, err := bg.GetBlock(bm)
	if err != nil {
		return err
	}
	if len(data) != bm.BlockSize {
		return fmt.Errorf("data did not match block size")
	}

	// TODO: validate sha512

	if n, err := w.WriteAt(data, int64(bm.offset)); err != nil {
		return err
	} else if n != bm.expSize {
		return fmt.Errorf("block %d did not write expected size %d, got %d", bm.Index, bm.expSize, n)
	}

	return nil
}
