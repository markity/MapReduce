package cmd

import (
	"fmt"
	"os"

	"mapreduce/server/worker/cmd/runtask"
	"mapreduce/server/worker/cmd/serve"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return newWorkerCommand("worker", []string{"worker", "run-task"})
}

func NewRootCommand() *cobra.Command {
	return newWorkerCommand("mapreduce-worker", []string{"run-task"})
}

func newWorkerCommand(use string, runTaskArgs []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: "MapReduce worker commands",
	}
	cmd.AddCommand(serve.NewCommand(runTaskArgs))
	cmd.AddCommand(runtask.NewCommand())
	return cmd
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
