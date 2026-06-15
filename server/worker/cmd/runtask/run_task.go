package runtask

import (
	"log"
	"mapreduce/server/worker/runner"
	"os"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	var taskSpecPath string
	cmd := &cobra.Command{
		Use:    "run-task",
		Hidden: true,
		Run: func(cmd *cobra.Command, args []string) {
			if taskSpecPath == "" {
				log.Println("task spec path must be specified")
				os.Exit(2)
			}
			if err := runner.RunTask(taskSpecPath); err != nil {
				log.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringVar(&taskSpecPath, "spec", "", "Task spec path")
	return cmd
}
