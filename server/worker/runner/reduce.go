package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	mrplugin "mapreduce/plugin"
	"mapreduce/server/worker/entity"
	"mapreduce/server/worker/spill"
)

func runReduceTask(spec TaskSpec, loaded *loadedPlugin, report entity.TaskReport) entity.TaskReport {
	if spec.Assign.ReduceTask == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "reduce task spec is nil"
		return report
	}
	if lost, err := fetchReduceInputs(spec); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		report.LostMapOutputs = lost
		return report
	}
	if loaded == nil || loaded.Plugin == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "plugin is not loaded"
		return report
	}
	if err := executeReduce(spec, loaded); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	return report
}

func executeReduce(spec TaskSpec, loaded *loadedPlugin) error {
	reducer := loaded.Plugin.Reducer()
	if reducer == nil {
		return fmt.Errorf("plugin reducer is nil")
	}
	groups, err := readReduceInputs(filepath.Join(spec.AttemptDir, "reduce-inputs"))
	if err != nil {
		return err
	}
	ctx, err := newReduceContext(filepath.Join(spec.AttemptDir, "reduce-output"), loaded.Conf)
	if err != nil {
		return err
	}
	defer ctx.Close()
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		if err := reducer.Reduce(group.key, group.values, ctx); err != nil {
			return err
		}
	}
	return nil
}

type reduceContext struct {
	conf mrplugin.Configuration
	file *os.File
}

func newReduceContext(outputPath string, conf mrplugin.Configuration) (*reduceContext, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	return &reduceContext{conf: conf, file: file}, nil
}

func (ctx *reduceContext) Write(key []byte, value []byte) error {
	_, err := fmt.Fprintf(ctx.file, "%s\t%s\n", string(key), string(value))
	return err
}

func (ctx *reduceContext) Configuration() mrplugin.Configuration {
	return ctx.conf
}

func (ctx *reduceContext) Close() error {
	return ctx.file.Close()
}

type reduceGroup struct {
	key    []byte
	values [][]byte
}

func readReduceInputs(inputDir string) (map[string]reduceGroup, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".index") {
			continue
		}
		paths = append(paths, filepath.Join(inputDir, name))
	}
	reader, err := spill.NewMergeReader(paths)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	groups := make(map[string]reduceGroup)
	for {
		ok, err := reader.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return groups, nil
		}
		rec := reader.Record()
		keyBytes := rec.Key
		valueBytes := rec.Value
		groupKey := string(keyBytes)
		group := groups[groupKey]
		if group.key == nil {
			group.key = keyBytes
		}
		group.values = append(group.values, valueBytes)
		groups[groupKey] = group
	}
}
