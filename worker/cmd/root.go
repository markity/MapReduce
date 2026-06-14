package cmd

import (
	"fmt"
	"log"
	"mapreduce/worker/runner"
	"mapreduce/worker/scheduler"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	uniqueID                 string
	masterAddr               string
	listenAddr               string
	advertiseAddr            string
	epoch                    int64
	dataDir                  string
	numSlots                 int
	heartbeatIntervalSeconds int
	mapSpillBufferMB         int
	taskSpecPath             string
)

var rootCmd = &cobra.Command{
	Use:   "mapreduce-worker",
	Short: "A MapReduce worker",
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
		}.NormalizedForCmd()

		sche := scheduler.NewScheduer(cfg)
		sche.Run()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&masterAddr, "master", "http://localhost:8080", "Master base URL")
	rootCmd.PersistentFlags().StringVar(&listenAddr, "listen", "localhost:9090", "Worker listen address")
	rootCmd.PersistentFlags().StringVar(&advertiseAddr, "advertise", "", "Worker address advertised to other workers; defaults to --listen")
	rootCmd.PersistentFlags().StringVar(&uniqueID, "worker-id", "", "Stable worker unique ID; defaults to hostname-pid")
	rootCmd.PersistentFlags().Int64Var(&epoch, "epoch", time.Now().UnixNano(), "Worker epoch, should increase after worker restart")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "./worker-data", "Worker local data directory")
	rootCmd.PersistentFlags().IntVar(&numSlots, "slots", 2, "Number of concurrent task slots")
	rootCmd.PersistentFlags().IntVar(&heartbeatIntervalSeconds, "heartbeat-interval-seconds", 1, "Heartbeat interval in seconds")
	rootCmd.PersistentFlags().IntVar(&mapSpillBufferMB, "map-spill-buffer-mb", 64, "Map output sort buffer size in MiB before spilling to disk")
	rootCmd.AddCommand(runTaskCmd)
}

var runTaskCmd = &cobra.Command{
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

func init() {
	runTaskCmd.Flags().StringVar(&taskSpecPath, "spec", "", "Task spec path")
}
