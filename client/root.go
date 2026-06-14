package main

import (
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

var (
	masterAddr string
	httpClient = &http.Client{Timeout: 30 * time.Second}
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce-client",
	Short: "MapReduce master client",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&masterAddr, "master", "http://localhost:9998", "Master base URL")
	rootCmd.AddCommand(
		listPluginsCmd,
		uploadPluginCmd,
		deletePluginCmd,
		listJobsCmd,
		createJobCmd,
		masterStateCmd,
	)
}
