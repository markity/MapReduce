package serve

import (
	"fmt"
	"os"

	"mapreduce/server/master/server"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	var listenAddr string
	var pluginStorePath string
	var pluginCleanupIntervalSeconds int
	var workerHeartbeatLostInternalSeconds int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MapReduce master server",
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
	cmd.Flags().StringVar(&listenAddr, "listen", "localhost:9998", "Server listen address")
	cmd.Flags().StringVar(&pluginStorePath, "plugin-store", "./plugins", "Directory used to store uploaded plugins")
	cmd.Flags().IntVar(&pluginCleanupIntervalSeconds, "plugin-cleanup-interval-seconds", 60, "Seconds between deleted plugin cleanup runs; <=0 disables background cleanup")
	cmd.Flags().IntVar(&workerHeartbeatLostInternalSeconds, "worker-heartbeat-lost-interval-seconds", 10, "Interval in seconds for scanning lost workers by heartbeat timeout; must be > 0")
	return cmd
}
