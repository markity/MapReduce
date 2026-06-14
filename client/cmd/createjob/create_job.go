package createjob

import (
	"encoding/json"
	"fmt"
	"mapreduce/client/internal/cli"
	mrplugin "mapreduce/plugin"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"
	"os"
	stdplugin "plugin"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	var jobName string
	var pluginID string
	var numReduce int
	var pluginArgs []string

	cmd := &cobra.Command{
		Use:   "create-job",
		Short: "Create a MapReduce job",
		RunE: func(cmd *cobra.Command, args []string) error {
			if pluginID == "" || numReduce <= 0 {
				return fmt.Errorf("--plugin and positive --num-reduce are required")
			}
			pluginArgs = append(pluginArgs, args...)
			pluginFile, cleanup, err := downloadPlugin(newClient(), pluginID)
			if err != nil {
				return err
			}
			defer cleanup()

			conf, splits, err := buildJobSpec(pluginFile, pluginArgs)
			if err != nil {
				return err
			}
			req := clientcall.CreateMapReduceJobReq{
				JobName:        jobName,
				PluginUniqueID: pluginID,
				NumReduceTasks: numReduce,
				Conf:           conf,
				TaskSplits:     splits,
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
	cmd.Flags().StringArrayVar(&pluginArgs, "args", nil, "Plugin BuildPlugin args; remaining positional values after --args are also appended")
	return cmd
}

func downloadPlugin(client *cli.Client, pluginID string) (string, func(), error) {
	data, err := client.DoBytes(http.MethodGet, "/client-api/fetch-plugin/"+url.PathEscape(pluginID), nil)
	if err != nil {
		return "", func() {}, err
	}
	file, err := os.CreateTemp("", "mapreduce-plugin-*.so")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		_ = os.Remove(file.Name())
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		cleanup()
		return "", func() {}, err
	}
	if err := file.Close(); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return file.Name(), cleanup, nil
}

func buildJobSpec(pluginFile string, args []string) (map[string]string, []comm.SplitSpec, error) {
	opened, err := stdplugin.Open(pluginFile)
	if err != nil {
		return nil, nil, err
	}
	sym, err := opened.Lookup("BuildPlugin")
	if err != nil {
		return nil, nil, err
	}
	build, ok := sym.(func([]string) (mrplugin.Configuration, mrplugin.JobPlugin, error))
	if !ok {
		return nil, nil, fmt.Errorf("BuildPlugin has unexpected signature")
	}
	conf, jobPlugin, err := build(args)
	if err != nil {
		return nil, nil, err
	}
	if conf == nil {
		conf = mrplugin.NewConfiguration()
	}
	if jobPlugin == nil || jobPlugin.InputFormat() == nil {
		return nil, nil, fmt.Errorf("plugin input format is nil")
	}
	pluginSplits, err := jobPlugin.InputFormat().GetSplits(conf)
	if err != nil {
		return nil, nil, err
	}
	splits := make([]comm.SplitSpec, 0, len(pluginSplits))
	for _, split := range pluginSplits {
		data, err := split.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
		splits = append(splits, comm.SplitSpec{
			SplitType: comm.SplitType(split.Kind()),
			Data:      data,
		})
	}
	if len(splits) == 0 {
		return nil, nil, fmt.Errorf("input format returned no splits")
	}
	return conf.ToMap(), splits, nil
}
