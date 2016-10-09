package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

func Main(args []string) error {
	if len(args) != 4 {
		return fmt.Errorf("Usage: %s put /path/to/local/file /path/on/remote/machine", args[0])
	}

	v := blob.NewInfluxVolume("http://localhost:8086", "blob", "")

	e, err := blob.NewEngine(blob.EngineConfig{
		UploadReaders: 2,
		UploadWriters: 2,
	})
	if err != nil {
		return err
	}

	in, err := os.Open(args[2])
	if err != nil {
		return err
	}

	m, err := blob.NewFileMeta(in)
	if err != nil {
		return err
	}
	m.Path = args[3]
	m.BlockSize = 1024
	m.Time = time.Now().Unix()

	progress, err := e.PutFile(in, m, v)
	if err != nil {
		return err
	}

	fmt.Println("Put initiated, waiting for completion.")
	progress.Wait()
	fmt.Println("Put complete!")

	return nil
}
