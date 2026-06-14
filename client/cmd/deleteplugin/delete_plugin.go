package deleteplugin

import (
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete-plugin <plugin-id>",
		Short: "Delete a plugin by id",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginID := args[0]
			var resp clientcall.DeletePluginResp
			if err := newClient().DoJSON(http.MethodDelete, "/client-api/plugin/"+url.PathEscape(pluginID), nil, &resp); err != nil {
				return err
			}
			return cli.PrintTable([]string{"STATUS", "PLUGIN-ID"}, [][]string{{resp.Msg, pluginID}})
		},
	}
	return cmd
}
