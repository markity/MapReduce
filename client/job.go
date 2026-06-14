package main

import (
	"encoding/json"
	"fmt"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	createJobJobName        string
	createJobPluginUniqueID string
	createJobNumReduce      int
	createJobConfFile       string
	createJobConfValues     []string
)

var listJobsCmd = &cobra.Command{
	Use:   "list-jobs",
	Short: "List jobs",
	RunE: func(cmd *cobra.Command, args []string) error {
		var resp clientcall.ListJobsResp
		if err := doJSON(http.MethodGet, "/client-api/jobs", nil, &resp); err != nil {
			return err
		}
		rows := make([][]string, 0, len(resp.Jobs))
		for _, job := range resp.Jobs {
			rows = append(rows, []string{job.JobID, job.JobName, job.Status})
		}
		return printTable([]string{"JOB-ID", "NAME", "STATUS"}, rows)
	},
}

var createJobCmd = &cobra.Command{
	Use:   "create-job",
	Short: "Create a MapReduce job",
	RunE: func(cmd *cobra.Command, args []string) error {
		if createJobPluginUniqueID == "" || createJobNumReduce <= 0 {
			return fmt.Errorf("--plugin and positive --num-reduce are required")
		}
		conf, err := readJobConf(createJobConfFile, createJobConfValues)
		if err != nil {
			return err
		}
		req := clientcall.CreateMapReduceJobReq{
			JobName:        createJobJobName,
			PluginUniqueID: createJobPluginUniqueID,
			NumReduceTasks: createJobNumReduce,
			Conf:           conf,
		}
		body, err := json.Marshal(req)
		if err != nil {
			return err
		}
		var resp clientcall.CreateMapReduceJobResp
		if err := doJSON(http.MethodPost, "/client-api/jobs", body, &resp); err != nil {
			return err
		}
		return printTable([]string{"STATUS", "JOB-ID"}, [][]string{{resp.Msg, resp.JobID}})
	},
}

func init() {
	createJobCmd.Flags().StringVar(&createJobJobName, "name", "", "Job name")
	createJobCmd.Flags().StringVar(&createJobPluginUniqueID, "plugin-id", "", "Plugin unique id")
	createJobCmd.Flags().IntVar(&createJobNumReduce, "num-reduce", 1, "Number of reduce tasks")
	createJobCmd.Flags().StringVar(&createJobConfFile, "conf-file", "", "JSON file containing object of string values")
	createJobCmd.Flags().StringArrayVar(&createJobConfValues, "conf", nil, "Configuration key=value, can be repeated")
}

func readJobConf(path string, values []string) (map[string]string, error) {
	conf := make(map[string]string)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &conf); err != nil {
			return nil, err
		}
	}
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid --conf %q, want key=value", raw)
		}
		conf[key] = value
	}
	return conf, nil
}
