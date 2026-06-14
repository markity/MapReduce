package deleteplugin

import (
	"fmt"
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	var pluginID string
	cmd := &cobra.Command{
		Use:   "delete-plugin",
		Short: "Delete a plugin by id",
		RunE: func(cmd *cobra.Command, args []string) error {
			if pluginID == "" {
				return fmt.Errorf("--plugin-id is required")
			}
			var resp clientcall.DeletePluginResp
			if err := newClient().DoJSON(http.MethodDelete, "/client-api/plugin/"+url.PathEscape(pluginID), nil, &resp); err != nil {
				return err
			}
			return cli.PrintTable([]string{"STATUS", "PLUGIN-ID"}, [][]string{{resp.Msg, pluginID}})
		},
	}
	cmd.Flags().StringVar(&pluginID, "plugin-id", "", "Plugin id")
	return cmd
}
