package main

import (
	"fmt"
	"mapreduce/client/cmd"
	"os"
)

func main() {
	if err := cmd.NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
