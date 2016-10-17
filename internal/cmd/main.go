package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/mark-rushakoff/influx-blob/blob"
	"github.com/mark-rushakoff/influx-blob/engine"
)

func Main(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("Usage: %s [up|down|ls] ARGS...", args[0])
	}

	v := blob.NewInfluxVolume("http://localhost:8086", "blob", "")

	e := engine.NewEngine(0, 0)

	var err error
	switch args[1] {
	case "up", "upload":
		err = up(args, e, v)
	case "down", "download":
		err = down(args, e, v)
	case "ls", "list":
		err = list(args, v)
	default:
		err = fmt.Errorf("Available commands: get, put")
	}
	return err
}

func up(args []string, e *engine.Engine, v *blob.InfluxVolume) error {
	if len(args) != 4 {
		return fmt.Errorf("Usage: %s up /path/to/local/file /path/on/remote/machine", args[0])
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

	ctx := e.UploadFile(in, fm, v)
	if err != nil {
		return err
	}

	fmt.Println("Put initiated, waiting for completion.")
	ctx.Wait()
	fmt.Println("Put complete!")

	stats := ctx.Stats()
	uploaders, _ := e.NumWorkers()
	fmt.Printf("Uploaded %d bytes in %.2fs\n", stats.Bytes, stats.Duration.Seconds())
	fmt.Printf("(Used %d uploaders and %d chunks of %dB each)\n", uploaders, fm.NumBlocks(), fm.BlockSize)

	return nil
}

func down(args []string, e *engine.Engine, v *blob.InfluxVolume) error {
	if len(args) != 4 {
		return fmt.Errorf("Usage: %s down /path/on/remote/machine /path/to/local/file", args[0])
	}

	bms, err := v.ListBlocks(args[2])
	if err != nil {
		return err
	}
	if len(bms) == 0 {
		return fmt.Errorf("No blocks found for path: %s", args[2])
	}

	out, err := os.OpenFile(args[3], os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer out.Close()

	// TODO: handle multiple FileMeta
	ctx, err := e.DownloadFile(out, bms, v)
	if err != nil {
		return err
	}

	fmt.Println("Get initiated, waiting for completion.")
	ctx.Wait()
	fmt.Println("Get complete!")

	fm := bms[0].FileMeta
	fmt.Println("Comparing checksum...")
	if err := fm.CompareSHA256Against(out); err != nil {
		return err
	}
	fmt.Println("Checksum matches. Get successful.")

	stats := ctx.Stats()
	_, downloaders := e.NumWorkers()
	fmt.Printf("Downloaded %d bytes in %.2fs\n", stats.Bytes, stats.Duration.Seconds())
	fmt.Printf("(Used %d downloaders and %d chunks of %dB each)\n", downloaders, fm.NumBlocks(), fm.BlockSize)

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
