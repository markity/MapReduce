package runner

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	mrplugin "mapreduce/plugin"
	"mapreduce/server/worker/entity"
	"mapreduce/server/worker/spill"
)

func runMapTask(spec TaskSpec, loaded *loadedPlugin, report entity.TaskReport) entity.TaskReport {
	if spec.Assign.MapTask == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "map task spec is nil"
		return report
	}
	if loaded == nil || loaded.Plugin == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "plugin is not loaded"
		return report
	}
	if err := executeMap(spec, loaded); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	size, err := mapOutputSize(spec.AttemptDir)
	if err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	report.MapOutput = &entity.MapOutputMetaEntry{
		WorkerUniqueID: spec.WorkerUniqueID,
		WorkerAddr:     spec.WorkerAddr,
		TaskAttemptKey: spec.Assign.TaskAttemptKey,
		Size:           size,
	}
	return report
}

func executeMap(spec TaskSpec, loaded *loadedPlugin) error {
	inputFormat := loaded.Plugin.InputFormat()
	if inputFormat == nil {
		return fmt.Errorf("plugin input format is nil")
	}
	split, err := inputFormat.NewSplit(spec.Assign.MapTask.Split.SplitType)
	if err != nil {
		return err
	}
	if err := split.UnmarshalBinary(spec.Assign.MapTask.Split.Data); err != nil {
		return err
	}
	reader, err := inputFormat.CreateRecordReader(split, loaded.Conf)
	if err != nil {
		return err
	}
	defer reader.Close()
	mapper := loaded.Plugin.Mapper()
	if mapper == nil {
		return fmt.Errorf("plugin mapper is nil")
	}
	ctx, err := newMapContext(spec.AttemptDir, spec.Assign.MapTask.NumReduce, spec.MapSpillBufferBytes, loaded)
	if err != nil {
		return err
	}
	for reader.Next() {
		key, value := reader.Record()
		if err := mapper.Map(key, value, ctx); err != nil {
			_ = ctx.Close()
			return err
		}
	}
	if err := reader.Err(); err != nil {
		_ = ctx.Close()
		return err
	}
	return ctx.Close()
}

func mapOutputSize(attemptDir string) (int64, error) {
	info, err := os.Stat(filepath.Join(attemptDir, "map.out"))
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

type mapContext struct {
	conf       mrplugin.Configuration
	attemptDir string
	buffer     *spill.MapOutputBuffer
}

func newMapContext(attemptDir string, numReduce int, spillBufferBytes int64, loaded *loadedPlugin) (*mapContext, error) {
	if numReduce <= 0 {
		return nil, fmt.Errorf("num reduce must be positive")
	}
	if spillBufferBytes <= 0 {
		spillBufferBytes = 64 << 20
	}
	if spillBufferBytes > int64(math.MaxInt) {
		return nil, fmt.Errorf("map spill buffer is too large: %d", spillBufferBytes)
	}
	buffer, err := spill.NewMapOutputBuffer(filepath.Join(attemptDir, "spill"), int(spillBufferBytes), numReduce, loaded.Plugin.Partitioner())
	if err != nil {
		return nil, err
	}
	return &mapContext{conf: loaded.Conf, attemptDir: attemptDir, buffer: buffer}, nil
}

func (ctx *mapContext) Write(key []byte, value []byte) error {
	return ctx.buffer.Emit(key, value)
}

func (ctx *mapContext) Configuration() mrplugin.Configuration {
	return ctx.conf
}

func (ctx *mapContext) Close() error {
	if _, err := ctx.buffer.Finish(filepath.Join(ctx.attemptDir, "map.out"), filepath.Join(ctx.attemptDir, "map.out.index")); err != nil {
		_ = ctx.buffer.Cleanup()
		return err
	}
	return ctx.buffer.Cleanup()
}
