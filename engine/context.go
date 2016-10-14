package engine

import (
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

type FileTransferContext struct {
	Blocks []*BlockTransferContext

	fm *blob.FileMeta
}

func (c *FileTransferContext) Wait() {
	for _, b := range c.Blocks {
		b.Wait()
	}
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
