package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"mapreduce/rpc/comm"
	"mapreduce/worker/entity"
	"mapreduce/worker/runner"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type taskProcess struct {
	Assign     entity.TaskSlotAssigned
	AttemptDir string
	ReportPath string
	Cmd        *exec.Cmd
}

type taskProcessResult struct {
	Key    entity.TaskAttemptKey
	Report entity.TaskReport
	Err    error
}

func (impl *schedulerImpl) handleAssignedTasks(tasks []comm.AssignTask) {
	for _, rpcTask := range tasks {
		assigned := entity.TaskSlotAssignedFromAssignedTask(entity.AssignedTaskFromRpcComm(rpcTask))
		if _, ok := impl.Processes[assigned.TaskAttemptKey]; ok {
			continue
		}
		if !impl.SlotTable.markRunning(assigned) {
			continue
		}
		process, err := impl.startTaskProcess(assigned)
		if err != nil {
			impl.SlotTable.releaseAttempt(assigned.TaskAttemptKey)
			impl.Reports.Add(impl.failedReportFromAssign(assigned, err))
			continue
		}
		impl.Processes[assigned.TaskAttemptKey] = process
	}
}

func (impl *schedulerImpl) handleShouldStopAttempts(keys []entity.TaskAttemptKey) {
	for _, key := range keys {
		process := impl.Processes[key]
		if process == nil {
			impl.SlotTable.releaseAttempt(key)
			continue
		}
		delete(impl.Processes, key)
		impl.SlotTable.releaseAttempt(key)
		go func(process *taskProcess) {
			if err := killTaskProcess(process); err != nil {
				log.Printf("kill task process failed: attempt=%+v err=%v", process.Assign.TaskAttemptKey, err)
			}
		}(process)
	}
}

func (impl *schedulerImpl) drainProcessResults() {
	for {
		select {
		case result := <-impl.ProcessResults:
			process := impl.Processes[result.Key]
			if process == nil {
				continue
			}
			delete(impl.Processes, result.Key)
			impl.SlotTable.releaseAttempt(result.Key)
			if result.Err != nil && result.Report.Status == "" {
				result.Report = impl.failedReportFromAssign(process.Assign, result.Err)
			}
			impl.Reports.Add(result.Report)
			if process.Assign.TaskType == entity.TaskTypeMap {
				impl.UncleanedMapHangAttempts[result.Key] = struct{}{}
			}
		default:
			return
		}
	}
}

func (impl *schedulerImpl) killAllProcesses() {
	for key, process := range impl.Processes {
		if err := killTaskProcess(process); err != nil {
			log.Printf("kill task process failed during shutdown: attempt=%+v err=%v", key, err)
		}
		delete(impl.Processes, key)
		impl.SlotTable.releaseAttempt(key)
	}
}

func (impl *schedulerImpl) startTaskProcess(assign entity.TaskSlotAssigned) (*taskProcess, error) {
	attemptDir, ok := impl.attemptLocalDataDir(assign.TaskAttemptKey)
	if !ok {
		return nil, fmt.Errorf("invalid attempt dir for %+v", assign.TaskAttemptKey)
	}
	if err := os.MkdirAll(attemptDir, 0o755); err != nil {
		return nil, err
	}

	reportPath := filepath.Join(attemptDir, "report.json")
	specPath := filepath.Join(attemptDir, "spec.json")
	stdoutPath := filepath.Join(attemptDir, "stdout.log")
	stderrPath := filepath.Join(attemptDir, "stderr.log")

	spec := runner.TaskSpec{
		Assign:         assign,
		AttemptDir:     attemptDir,
		ReportPath:     reportPath,
		MasterAddr:     impl.Cfg.MasterAddr,
		WorkerUniqueID: impl.Cfg.WorkerUniqueID,
		WorkerAddr:     impl.Cfg.AdvertiseAddr,
	}
	specData, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(specPath, specData, 0o644); err != nil {
		return nil, err
	}

	cmd := exec.Command(os.Args[0], "run-task", "--spec", specPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := os.Create(stdoutPath)
	if err != nil {
		return nil, err
	}
	stderr, err := os.Create(stderrPath)
	if err != nil {
		_ = stdout.Close()
		return nil, err
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	process := &taskProcess{
		Assign:     assign,
		AttemptDir: attemptDir,
		ReportPath: reportPath,
		Cmd:        cmd,
	}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	go func() {
		err := cmd.Wait()
		_ = stdout.Close()
		_ = stderr.Close()
		report, readErr := readTaskReport(reportPath)
		if readErr != nil && err == nil {
			err = readErr
		}
		impl.ProcessResults <- taskProcessResult{
			Key:    assign.TaskAttemptKey,
			Report: report,
			Err:    err,
		}
	}()
	return process, nil
}

func readTaskReport(reportPath string) (entity.TaskReport, error) {
	data, err := os.ReadFile(reportPath)
	if err != nil {
		return entity.TaskReport{}, err
	}
	var report entity.TaskReport
	if err := json.Unmarshal(data, &report); err != nil {
		return entity.TaskReport{}, err
	}
	return report, nil
}

func (impl *schedulerImpl) failedReportFromAssign(assign entity.TaskSlotAssigned, err error) entity.TaskReport {
	return entity.TaskReport{
		TaskAttemptKey: assign.TaskAttemptKey,
		WorkerUniqueID: impl.Cfg.WorkerUniqueID,
		WorkerAddr:     impl.Cfg.AdvertiseAddr,
		TaskType:       assign.TaskType,
		Status:         entity.TaskReportFailed,
		Error:          err.Error(),
	}
}

func killTaskProcess(process *taskProcess) error {
	if process.Cmd == nil || process.Cmd.Process == nil {
		return nil
	}
	pid := process.Cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}
	time.Sleep(2 * time.Second)
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}
