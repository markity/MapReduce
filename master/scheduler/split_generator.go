package scheduler

import (
	"fmt"
	"mapreduce/master/entity"
	mrplugin "mapreduce/plugin"
	stdplugin "plugin"
)

func generateJobSplits(pluginPath string, confValues map[string]string) ([]entity.SplitSpec, error) {
	opened, err := stdplugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}
	sym, err := opened.Lookup("BuildPlugin")
	if err != nil {
		return nil, err
	}
	build, ok := sym.(func([]string) (mrplugin.Configuration, mrplugin.JobPlugin, error))
	if !ok {
		return nil, fmt.Errorf("BuildPlugin has unexpected signature")
	}
	conf, jobPlugin, err := build(nil)
	if err != nil {
		return nil, err
	}
	if conf == nil {
		conf = mrplugin.NewConfiguration()
	}
	for key, value := range confValues {
		if err := conf.Set(key, value); err != nil {
			return nil, err
		}
	}
	if jobPlugin == nil || jobPlugin.InputFormat() == nil {
		return nil, fmt.Errorf("plugin input format is nil")
	}
	pluginSplits, err := jobPlugin.InputFormat().GetSplits(conf)
	if err != nil {
		return nil, err
	}
	splits := make([]entity.SplitSpec, 0, len(pluginSplits))
	for _, split := range pluginSplits {
		data, err := split.MarshalBinary()
		if err != nil {
			return nil, err
		}
		splits = append(splits, entity.SplitSpec{
			SplitType: split.Kind(),
			Data:      data,
		})
	}
	if len(splits) == 0 {
		return nil, fmt.Errorf("input format returned no splits")
	}
	return splits, nil
}
