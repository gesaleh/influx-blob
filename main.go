package main

import (
	"fmt"
	"os"

	"github.com/mark-rushakoff/influx-blob/internal/cmd"
)

func main() {
	if err := cmd.Main(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
}
