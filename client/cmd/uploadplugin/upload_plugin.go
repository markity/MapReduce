package uploadplugin

import (
	"fmt"
	"mapreduce/client/internal/cli"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

func NewCommand(newClient func() *cli.Client) *cobra.Command {
	var pluginName string
	var pluginFile string
	cmd := &cobra.Command{
		Use:   "upload-plugin",
		Short: "Upload a plugin .so file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if pluginName == "" || pluginFile == "" {
				return fmt.Errorf("--name and --file are required")
			}
			data, err := os.ReadFile(pluginFile)
			if err != nil {
				return err
			}
			var resp clientcall.UploadPluginResp
			if err := newClient().DoJSON(http.MethodPost, "/client-api/upload-plugin/"+url.PathEscape(pluginName), data, &resp); err != nil {
				return err
			}
			return cli.PrintTable([]string{"STATUS", "PLUGIN-ID"}, [][]string{{
				resp.Msg,
				resp.PluginUniqueID,
			}})
		},
	}
	cmd.Flags().StringVar(&pluginName, "name", "", "User plugin name")
	cmd.Flags().StringVar(&pluginFile, "file", "", "Plugin .so file path")
	return cmd
}
