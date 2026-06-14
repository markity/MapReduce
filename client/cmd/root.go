package cmd

import (
	"mapreduce/client/cmd/createjob"
	"mapreduce/client/cmd/deleteplugin"
	"mapreduce/client/cmd/listjobs"
	"mapreduce/client/cmd/listplugins"
	"mapreduce/client/cmd/masterstate"
	"mapreduce/client/cmd/uploadplugin"
	"mapreduce/client/internal/cli"

	"github.com/spf13/cobra"
)

var masterAddr string

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "mapreduce-client",
		Short: "MapReduce master client",
	}
	rootCmd.PersistentFlags().StringVar(&masterAddr, "master", "http://localhost:9998", "Master base URL")
	rootCmd.AddCommand(
		listplugins.NewCommand(newClient),
		uploadplugin.NewCommand(newClient),
		deleteplugin.NewCommand(newClient),
		listjobs.NewCommand(newClient),
		createjob.NewCommand(newClient),
		masterstate.NewCommand(newClient),
	)
	return rootCmd
}

func newClient() *cli.Client {
	return cli.NewClient(masterAddr)
}
