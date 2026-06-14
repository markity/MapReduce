package clientapis

import (
	"context"
	"fmt"
	mrplugin "mapreduce/plugin"
	"os"
	"os/exec"
	stdplugin "plugin"
	"strings"
	"time"
)

const validatePluginPathEnv = "MAPREDUCE_VALIDATE_PLUGIN_PATH"
const validatePluginTimeout = 10 * time.Second

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
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), validatePluginTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, exe)
	cmd.Env = append(os.Environ(), validatePluginPathEnv+"="+path)
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("plugin validation timed out")
	}
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			return err
		}
		return fmt.Errorf("%v: %s", err, msg)
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
