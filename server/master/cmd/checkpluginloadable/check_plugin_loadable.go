package checkpluginloadable

import (
	"fmt"
	mrplugin "mapreduce/plugin"
	stdplugin "plugin"

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
			return checkLoadable(args[0])
		},
	}
}

func checkLoadable(path string) error {
	opened, err := stdplugin.Open(path)
	if err != nil {
		return err
	}
	sym, err := opened.Lookup("BuildPlugin")
	if err != nil {
		return err
	}
	if _, ok := sym.(func([]string) (mrplugin.Configuration, mrplugin.JobPlugin, error)); !ok {
		return fmt.Errorf("BuildPlugin has unexpected signature")
	}
	return nil
}
