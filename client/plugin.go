package main

import (
	"fmt"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

var (
	uploadPluginName           string
	uploadPluginFile           string
	pluginListOrder            string
	deletePluginPluginUniqueID string
)

var listPluginsCmd = &cobra.Command{
	Use:   "list-plugins",
	Short: "List visible plugins",
	RunE: func(cmd *cobra.Command, args []string) error {
		var resp clientcall.ListPluginsResp
		if err := doJSON(http.MethodGet, "/client-api/plugins/"+url.PathEscape(pluginListOrder), nil, &resp); err != nil {
			return err
		}
		rows := make([][]string, 0, len(resp.PluginInfos))
		if len(resp.PluginInfos) > 0 {
			for _, plugin := range resp.PluginInfos {
				rows = append(rows, []string{plugin.PluginUniqueID, plugin.UploadedAt})
			}
		} else {
			for _, plugin := range resp.Plugins {
				rows = append(rows, []string{plugin, ""})
			}
		}
		return printTable([]string{"PLUGIN-ID", "UPLOADED-AT"}, rows)
	},
}

var uploadPluginCmd = &cobra.Command{
	Use:   "upload-plugin",
	Short: "Upload a plugin .so file",
	RunE: func(cmd *cobra.Command, args []string) error {
		if uploadPluginName == "" || uploadPluginFile == "" {
			return fmt.Errorf("--name and --file are required")
		}
		data, err := os.ReadFile(uploadPluginFile)
		if err != nil {
			return err
		}
		var resp clientcall.UploadPluginResp
		if err := doJSON(http.MethodPost, "/client-api/upload-plugin/"+url.PathEscape(uploadPluginName), data, &resp); err != nil {
			return err
		}
		return printTable([]string{"STATUS", "PLUGIN-ID"}, [][]string{{resp.Msg, resp.PluginUniqueID}})
	},
}

var deletePluginCmd = &cobra.Command{
	Use:   "delete-plugin",
	Short: "Delete a plugin by id",
	RunE: func(cmd *cobra.Command, args []string) error {
		if uploadPluginName == "" {
			return fmt.Errorf("--plugin-id is required")
		}
		var resp clientcall.DeletePluginResp
		if err := doJSON(http.MethodDelete, "/client-api/plugin/"+url.PathEscape(deletePluginPluginUniqueID), nil, &resp); err != nil {
			return err
		}
		fmt.Println("ok")
		return nil
	},
}

func init() {
	listPluginsCmd.Flags().StringVar(&pluginListOrder, "order", "dict-order", "Plugin list order: dict-order or time-order")

	uploadPluginCmd.Flags().StringVar(&uploadPluginName, "name", "", "User plugin name")
	uploadPluginCmd.Flags().StringVar(&uploadPluginFile, "file", "", "Plugin .so file path")

	deletePluginCmd.Flags().StringVar(&uploadPluginName, "name", "", "Plugin name")
	deletePluginCmd.Flags().StringVar(&deletePluginPluginUniqueID, "plugin-id", "", "Plugin unique id")

}
