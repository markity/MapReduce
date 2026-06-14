package listplugins

import (
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	var order string
	cmd := &cobra.Command{
		Use:   "list-plugins",
		Short: "List visible plugins",
		RunE: func(cmd *cobra.Command, args []string) error {
			var resp clientcall.ListPluginsResp
			if err := newClient().DoJSON(http.MethodGet, "/client-api/plugins/"+url.PathEscape(order), nil, &resp); err != nil {
				return err
			}
			rows := make([][]string, 0, len(resp.PluginInfos))
			if len(resp.PluginInfos) > 0 {
				for _, plugin := range resp.PluginInfos {
					rows = append(rows, []string{
						plugin.PluginUniqueID,
						cli.FormatDisplayTime(plugin.UploadedAt),
					})
				}
			} else {
				for _, plugin := range resp.Plugins {
					rows = append(rows, []string{plugin, ""})
				}
			}
			return cli.PrintTable([]string{"PLUGIN-ID", "UPLOADED-AT"}, rows)
		},
	}
	cmd.Flags().StringVar(&order, "order", "dict-order", "Plugin list order: dict-order or time-order")
	return cmd
}
