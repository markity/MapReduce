package listjobs

import (
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "list-jobs",
		Short: "List jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp clientcall.ListJobsResp
			if err := newClient().DoJSON(http.MethodGet, "/client-api/jobs", nil, &resp); err != nil {
				return err
			}
			rows := make([][]string, 0, len(resp.Jobs))
			for _, job := range resp.Jobs {
				rows = append(rows, []string{job.JobID, job.JobName, job.Status})
			}
			return cli.PrintTable([]string{"JOB-ID", "NAME", "STATUS"}, rows)
		},
	}
}
