package cmd

import (
	"fmt"
	"os"

	"mapreduce/master/cmd/checkpluginloadable"
	"mapreduce/master/cmd/serve"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce",
	Short: "A MapReduce master server",
	Long: `MapReduce master server

A distributed computing framework for processing large datasets.`,
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
