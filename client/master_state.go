package main

import (
	"fmt"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var masterStateCmd = &cobra.Command{
	Use:   "master-state",
	Short: "Get master internal state snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		var resp clientcall.GetMasterStateResp
		if err := doJSON(http.MethodGet, "/client-api/master-state", nil, &resp); err != nil {
			return err
		}
		return printMasterState(resp)
	},
}

func printMasterState(resp clientcall.GetMasterStateResp) error {
	if err := printWorkers(resp.Workers); err != nil {
		return err
	}
	fmt.Println()
	if err := printPlugins(resp.Plugins); err != nil {
		return err
	}
	fmt.Println()
	return printStateJobs(resp.Jobs)
}

func printWorkers(workers []clientcall.MasterStateWorkerInfo) error {
	rows := make([][]string, 0, len(workers))
	for _, worker := range workers {
		rows = append(rows, []string{
			worker.WorkerUniqueID,
			worker.WorkerAddr,
			worker.WorkerState,
			fmt.Sprint(worker.WorkerEpoch),
			fmt.Sprint(worker.LastSeq),
			strings.Join(worker.FreeSlots, ","),
			strings.Join(worker.BusySlots, ","),
			worker.LastSeen,
		})
	}
	return printTable([]string{"WORKER ID", "ADDR", "STATE", "EPOCH", "SEQ", "FREE", "BUSY", "LAST SEEN"}, rows)
}

func printPlugins(plugins []clientcall.MasterStatePluginInfo) error {
	rows := make([][]string, 0, len(plugins))
	for _, plugin := range plugins {
		rows = append(rows, []string{
			plugin.PluginUniqueID,
			fmt.Sprint(plugin.PinCount),
			plugin.ModTime,
		})
	}
	return printTable([]string{"PLUGIN-ID", "PINS", "MOD TIME"}, rows)
}

func printStateJobs(jobs []clientcall.MasterStateJobInfo) error {
	rows := make([][]string, 0, len(jobs))
	for _, job := range jobs {
		rows = append(rows, []string{
			job.JobID,
			job.JobName,
			job.Status,
			job.StartedAt,
		})
	}
	return printTable([]string{"JOB ID", "NAME", "STATUS", "STARTED"}, rows)
}
