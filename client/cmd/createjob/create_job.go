package createjob

import (
	"encoding/json"
	"fmt"
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	var jobName string
	var pluginID string
	var numReduce int
	var confFile string
	var confValues []string

	cmd := &cobra.Command{
		Use:   "create-job",
		Short: "Create a MapReduce job",
		RunE: func(cmd *cobra.Command, args []string) error {
			if pluginID == "" || numReduce <= 0 {
				return fmt.Errorf("--plugin and positive --num-reduce are required")
			}
			conf, err := readJobConf(confFile, confValues)
			if err != nil {
				return err
			}
			req := clientcall.CreateMapReduceJobReq{
				JobName:        jobName,
				PluginUniqueID: pluginID,
				NumReduceTasks: numReduce,
				Conf:           conf,
			}
			body, err := json.Marshal(req)
			if err != nil {
				return err
			}
			var resp clientcall.CreateMapReduceJobResp
			if err := newClient().DoJSON(http.MethodPost, "/client-api/jobs", body, &resp); err != nil {
				return err
			}
			return cli.PrintTable([]string{"STATUS", "JOB-ID"}, [][]string{{resp.Msg, resp.JobID}})
		},
	}

	cmd.Flags().StringVar(&jobName, "name", "", "Job name")
	cmd.Flags().StringVar(&pluginID, "plugin", "", "Plugin id")
	cmd.Flags().IntVar(&numReduce, "num-reduce", 1, "Number of reduce tasks")
	cmd.Flags().StringVar(&confFile, "conf-file", "", "JSON file containing object of string values")
	cmd.Flags().StringArrayVar(&confValues, "conf", nil, "Configuration key=value, can be repeated")
	return cmd
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
