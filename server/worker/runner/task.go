package runner

import (
	"encoding/json"
	"errors"
	"os"

	"mapreduce/server/worker/entity"
)

func RunTask(specPath string) error {
	spec, err := readTaskSpec(specPath)
	if err != nil {
		return err
	}
	report := runTask(spec)
	if err := writeTaskReport(spec.ReportPath, report); err != nil {
		return err
	}
	if report.Status == entity.TaskReportFailed {
		return errors.New(report.Error)
	}
	return nil
}

func readTaskSpec(path string) (TaskSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return TaskSpec{}, err
	}
	var spec TaskSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return TaskSpec{}, err
	}
	return spec, nil
}

func runTask(spec TaskSpec) entity.TaskReport {
	report := baseReport(spec)
	if err := os.MkdirAll(spec.AttemptDir, 0o755); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	var loaded *loadedPlugin
	pluginPath, err := materializePlugin(spec)
	if err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	loaded, err = loadPlugin(pluginPath, spec.Assign.Conf)
	if err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}

	switch spec.Assign.TaskType {
	case entity.TaskTypeMap:
		return runMapTask(spec, loaded, report)
	case entity.TaskTypeReduce:
		return runReduceTask(spec, loaded, report)
	default:
		report.Status = entity.TaskReportFailed
		report.Error = "unknown task type: " + string(spec.Assign.TaskType)
		return report
	}
}

func baseReport(spec TaskSpec) entity.TaskReport {
	return entity.TaskReport{
		TaskAttemptKey: spec.Assign.TaskAttemptKey,
		WorkerUniqueID: spec.WorkerUniqueID,
		WorkerAddr:     spec.WorkerAddr,
		TaskType:       spec.Assign.TaskType,
		Status:         entity.TaskReportSucceeded,
	}
}
