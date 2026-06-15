package plugincheck

import (
	"fmt"
	mrplugin "mapreduce/plugin"
	stdplugin "plugin"
)

func CheckLoadable(path string) error {
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
