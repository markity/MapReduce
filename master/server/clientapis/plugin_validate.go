package clientapis

import (
	"fmt"
	mrplugin "mapreduce/plugin"
	"os"
	"os/exec"
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
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), validatePluginPathEnv+"="+path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("plugin is not loadable: %w: %s", err, string(output))
	}
	return nil
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
