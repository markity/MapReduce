package cmd

import (
	"fmt"
	"os"

	"mapreduce/master/server"

	"github.com/spf13/cobra"
)

var (
	listenAddr                         string
	pluginStorePath                    string
	pluginCleanupIntervalSeconds       int
	workerHeartbeatLostInternalSeconds int
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce",
	Short: "A MapReduce master server",
	Long: `MapReduce master server

A distributed computing framework for processing large datasets.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("MapReduce Master api server listening on %s\n", listenAddr)
		if workerHeartbeatLostInternalSeconds <= 0 {
			panic("worker-heartbeat-lost-interval-seconds > 0 is needed")
		}
		srv := server.NewServer(listenAddr, pluginStorePath, pluginCleanupIntervalSeconds, workerHeartbeatLostInternalSeconds)
		if err := srv.Start(); err != nil {
			fmt.Printf("Error starting server: %v\n", err)
			os.Exit(1)
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&listenAddr, "listen", "localhost:9998", "Server listen address")
	rootCmd.PersistentFlags().StringVar(&pluginStorePath, "plugin-store", "./plugins", "Directory used to store uploaded plugins")
	rootCmd.PersistentFlags().IntVar(&pluginCleanupIntervalSeconds, "plugin-cleanup-interval-seconds", 60, "Seconds between deleted plugin cleanup runs; <=0 disables background cleanup")
	rootCmd.PersistentFlags().IntVar(&workerHeartbeatLostInternalSeconds, "worker-heartbeat-lost-interval-seconds", 10, "Interval in seconds for scanning lost workers by heartbeat timeout; must be > 0")
}
