package cmd

import (
	"fmt"
	"os"

	mastercmd "mapreduce/server/master/cmd"
	workercmd "mapreduce/server/worker/cmd"

	"github.com/spf13/cobra"
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "mr-server",
		Short: "MapReduce server command",
	}
	rootCmd.AddCommand(
		mastercmd.NewCommand(),
		workercmd.NewCommand(),
	)
	return rootCmd
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
