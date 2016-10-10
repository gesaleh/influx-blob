package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

func Main(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("Usage: %s [put|get] ARGS...", args[0])
	}

	v := blob.NewInfluxVolume("http://localhost:8086", "blob", "")

	e, err := blob.NewEngine(blob.EngineConfig{
		UploadReaders: 2,
		UploadWriters: 2,
	})
	if err != nil {
		return err
	}

	err = nil
	switch args[1] {
	case "put":
		err = put(args, e, v)
	case "get":
		err = get(args, e, v)
	default:
		err = fmt.Errorf("Available commands: get, put")
	}
	return err
}

func put(args []string, e *blob.Engine, v *blob.InfluxVolume) error {
	if len(args) != 4 {
		return fmt.Errorf("Usage: %s put /path/to/local/file /path/on/remote/machine", args[0])
	}

	in, err := os.Open(args[2])
	if err != nil {
		return err
	}
	defer in.Close()

	fm, err := blob.NewFileMeta(in)
	if err != nil {
		return err
	}
	fm.Path = args[3]
	fm.BlockSize = 1024
	fm.Time = time.Now().Unix()

	progress, err := e.PutFile(in, fm, v)
	if err != nil {
		return err
	}

	fmt.Println("Put initiated, waiting for completion.")
	progress.Wait()
	fmt.Println("Put complete!")

	return nil
}

func get(args []string, e *blob.Engine, v *blob.InfluxVolume) error {
	if len(args) != 4 {
		return fmt.Errorf("Usage: %s get /path/on/remote/machine /path/to/local/file", args[0])
	}

	bms, err := v.ListBlocks(args[2])
	if err != nil {
		return err
	}
	if len(bms) == 0 {
		return fmt.Errorf("No blocks found for path: %s", args[2])
	}

	// Open the file write-only, must not already exist.
	out, err := os.OpenFile(args[3], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer out.Close()

	// TODO: handle multiple FileMeta
	progress, err := e.GetBlocks(out, bms, v)
	if err != nil {
		return err
	}

	fmt.Println("Get initiated, waiting for completion.")
	progress.Wait()
	fmt.Println("Get complete!")

	return nil
}
