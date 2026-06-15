package checkpluginloadable

import (
	"mapreduce/server/master/plugincheck"

	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:           "check-plugin-loadable <plugin-path>",
		Short:         "Check whether a plugin can be loaded",
		Hidden:        true,
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return plugincheck.CheckLoadable(args[0])
		},
	}
}
