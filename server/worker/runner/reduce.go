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
	ctx, err := newReduceContext(
		filepath.Join(spec.AttemptDir, "reduce-output"),
		loaded.Conf,
		loaded.Plugin.OutputFormat(),
	)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = ctx.Abort()
		}
	}()
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
	if err := ctx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

type reduceContext struct {
	conf        mrplugin.Configuration
	outputPath  string
	writer      mrplugin.RecordWriter
	committer   mrplugin.OutputCommitter
	writerClose bool
	finished    bool
}

func newReduceContext(outputPath string, conf mrplugin.Configuration, outputFormat mrplugin.OutputFormat) (*reduceContext, error) {
	if outputFormat == nil {
		outputFormat = textOutputFormat{}
	}
	committer := outputFormat.OutputCommitter()
	if committer == nil {
		committer = noopOutputCommitter{}
	}
	if err := committer.SetupTask(outputPath, conf); err != nil {
		return nil, err
	}
	writer, err := outputFormat.CreateRecordWriter(outputPath, conf)
	if err != nil {
		_ = committer.AbortTask(outputPath, conf)
		return nil, err
	}
	if writer == nil {
		_ = committer.AbortTask(outputPath, conf)
		return nil, fmt.Errorf("output format returned nil record writer")
	}
	return &reduceContext{
		conf:       conf,
		outputPath: outputPath,
		writer:     writer,
		committer:  committer,
	}, nil
}

func (ctx *reduceContext) Write(key []byte, value []byte) error {
	return ctx.writer.Write(key, value)
}

func (ctx *reduceContext) Configuration() mrplugin.Configuration {
	return ctx.conf
}

func (ctx *reduceContext) closeWriter() error {
	if ctx.writerClose {
		return nil
	}
	ctx.writerClose = true
	return ctx.writer.Close()
}

func (ctx *reduceContext) Commit() error {
	if ctx.finished {
		return nil
	}
	if err := ctx.closeWriter(); err != nil {
		_ = ctx.committer.AbortTask(ctx.outputPath, ctx.conf)
		ctx.finished = true
		return err
	}
	if err := ctx.committer.CommitTask(ctx.outputPath, ctx.conf); err != nil {
		_ = ctx.committer.AbortTask(ctx.outputPath, ctx.conf)
		ctx.finished = true
		return err
	}
	ctx.finished = true
	return nil
}

func (ctx *reduceContext) Abort() error {
	if ctx.finished {
		return nil
	}
	closeErr := ctx.closeWriter()
	abortErr := ctx.committer.AbortTask(ctx.outputPath, ctx.conf)
	ctx.finished = true
	if closeErr != nil {
		return closeErr
	}
	return abortErr
}

type textOutputFormat struct{}

func (textOutputFormat) CreateRecordWriter(outputPath string, conf mrplugin.Configuration) (mrplugin.RecordWriter, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	return &textRecordWriter{file: file}, nil
}

func (textOutputFormat) OutputCommitter() mrplugin.OutputCommitter {
	return noopOutputCommitter{}
}

type noopOutputCommitter struct{}

func (noopOutputCommitter) SetupTask(outputPath string, conf mrplugin.Configuration) error {
	return nil
}

func (noopOutputCommitter) CommitTask(outputPath string, conf mrplugin.Configuration) error {
	return nil
}

func (noopOutputCommitter) AbortTask(outputPath string, conf mrplugin.Configuration) error {
	return nil
}

type textRecordWriter struct {
	file *os.File
}

func (w *textRecordWriter) Write(key []byte, value []byte) error {
	_, err := fmt.Fprintf(w.file, "%s\t%s\n", string(key), string(value))
	return err
}

func (w *textRecordWriter) Close() error {
	return w.file.Close()
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
