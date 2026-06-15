package serve

import (
	"log"
	"mapreduce/server/worker/scheduler"
	"time"

	"github.com/spf13/cobra"
)

func NewCommand(runTaskArgs []string) *cobra.Command {
	var uniqueID string
	var masterAddr string
	var listenAddr string
	var advertiseAddr string
	var epoch int64
	var dataDir string
	var numSlots int
	var heartbeatIntervalSeconds int
	var mapSpillBufferMB int

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the MapReduce worker",
		Run: func(cmd *cobra.Command, args []string) {
			if uniqueID == "" {
				log.Println("worker unique id must be specified")
				return
			}
			cfg := scheduler.Config{
				MasterAddr:        masterAddr,
				ListenAddr:        listenAddr,
				AdvertiseAddr:     advertiseAddr,
				WorkerUniqueID:    uniqueID,
				WorkerEpoch:       epoch,
				DataDir:           dataDir,
				NumSlots:          numSlots,
				HeartbeatInterval: time.Duration(heartbeatIntervalSeconds) * time.Second,
				MapSpillBufferMB:  mapSpillBufferMB,
				RunTaskArgs:       append([]string{}, runTaskArgs...),
			}.NormalizedForCmd()

			sche := scheduler.NewScheduer(cfg)
			sche.Run()
		},
	}
	cmd.Flags().StringVar(&masterAddr, "master", "http://localhost:8080", "Master base URL")
	cmd.Flags().StringVar(&listenAddr, "listen", "localhost:9090", "Worker listen address")
	cmd.Flags().StringVar(&advertiseAddr, "advertise", "", "Worker address advertised to other workers; defaults to --listen")
	cmd.Flags().StringVar(&uniqueID, "worker-id", "", "Stable worker unique ID; defaults to hostname-pid")
	cmd.Flags().Int64Var(&epoch, "epoch", time.Now().UnixNano(), "Worker epoch, should increase after worker restart")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./worker-data", "Worker local data directory")
	cmd.Flags().IntVar(&numSlots, "slots", 2, "Number of concurrent task slots")
	cmd.Flags().IntVar(&heartbeatIntervalSeconds, "heartbeat-interval-seconds", 1, "Heartbeat interval in seconds")
	cmd.Flags().IntVar(&mapSpillBufferMB, "map-spill-buffer-mb", 64, "Map output sort buffer size in MiB before spilling to disk")
	return cmd
}
