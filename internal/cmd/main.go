package cmd

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
)

func Main(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("Usage: %s [put|get|ls] ARGS...", args[0])
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
	case "ls", "list":
		err = list(args, v)
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
	out, err := os.OpenFile(args[3], os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
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

	h := sha256.New()
	if _, err := io.Copy(h, out); err != nil {
		return err
	}

	sha := bms[0].FileMeta.SHA256[:]
	if !bytes.Equal(h.Sum(nil), sha) {
		return fmt.Errorf("exp file checksum %x, got %x", sha, h.Sum(nil))
	}

	return nil
}

// list shows all files that match the supplied prefix.
func list(args []string, v *blob.InfluxVolume) error {
	if l := len(args); l != 2 && l != 3 {
		return fmt.Errorf("Usage: %s inspect [/path/prefix]", args[0])
	}

	pattern := "/"
	if len(args) == 3 {
		pattern = args[2]
	}
	files, err := v.ListFiles(pattern, blob.ListOptions{
		ListMatch: blob.ByPrefix,
	})
	if err != nil {
		return err
	}

	for _, f := range files {
		fmt.Println(f)
	}

	return nil
}
