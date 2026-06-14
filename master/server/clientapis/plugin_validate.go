package clientapis

import (
	"fmt"
	mrplugin "mapreduce/plugin"
	"os"
	"plugin"
	stdplugin "plugin"
)

const validatePluginPathEnv = "MAPREDUCE_VALIDATE_PLUGIN_PATH"

func init() {
	path := os.Getenv(validatePluginPathEnv)
	if path == "" {
		return
	}
	if err := validatePluginLoadableInProcess(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func validatePluginLoadable(path string) error {
	_, err := plugin.Open(path)
	return err
}

func validatePluginLoadableInProcess(path string) error {
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
