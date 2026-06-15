package cmd

import (
	"fmt"
	"os"

	"mapreduce/server/master/cmd/checkpluginloadable"
	"mapreduce/server/master/cmd/serve"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce-master",
	Short: "A MapReduce master server",
	Long: `MapReduce master server

A distributed computing framework for processing large datasets.`,
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "master",
		Short: "MapReduce master commands",
	}
	cmd.AddCommand(checkpluginloadable.NewCommand())
	cmd.AddCommand(serve.NewCommand())
	return cmd
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(checkpluginloadable.NewCommand())
	rootCmd.AddCommand(serve.NewCommand())
}
