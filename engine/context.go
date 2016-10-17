package engine

import (
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

type FileTransferContext struct {
	Blocks []*BlockTransferContext

	fm *blob.FileMeta
}

// Blocks execution until all underlying blocks have transferred.
// Returns immediately on subsequent calls. Safe for concurrent use.
func (c *FileTransferContext) Wait() {
	for _, b := range c.Blocks {
		b.Wait()
	}
}

// Returns a FileTransferStats indicating the duration of the transfer
// and the total bytes transferred. Not safe to call until Wait returns.
func (c *FileTransferContext) Stats() *FileTransferStats {
	if len(c.Blocks) == 0 {
		// Shouldn't call us if that's the case!
		return nil
	}

	firstStart := c.Blocks[0].startedAt
	lastFinish := c.Blocks[0].finishedAt

	for _, b := range c.Blocks {
		if b.startedAt.Before(firstStart) {
			firstStart = b.startedAt
		}
		if b.finishedAt.After(lastFinish) {
			lastFinish = b.finishedAt
		}
	}

	return &FileTransferStats{
		Duration: lastFinish.Sub(firstStart),
		Bytes:    c.fm.Size,
	}
}

type FileTransferStats struct {
	Duration time.Duration
	Bytes    int
}

type BlockTransferContext struct {
	startedAt  time.Time
	finishedAt time.Time
	done       chan struct{}

	bm *blob.BlockMeta
}

func (c *BlockTransferContext) Done() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *BlockTransferContext) Wait() {
	<-c.done
}
