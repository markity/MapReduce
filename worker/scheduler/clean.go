package scheduler

import (
	"fmt"
	"log"
	"mapreduce/worker/entity"
	"os"
	"path/filepath"
)

func (impl *schedulerImpl) handleCleanableJobs(jobIDs []string) {
	for _, jobID := range jobIDs {
		if jobID == "" {
			continue
		}
		impl.cleanUncleanedMapHangAttemptsForJob(jobID)
	}
}

func (impl *schedulerImpl) cleanUncleanedMapHangAttemptsForJob(jobID string) {
	for attempt := range impl.UncleanedMapHangAttempts {
		if attempt.JobID != jobID {
			continue
		}
		if err := impl.cleanAttemptLocalFiles(attempt); err != nil {
			log.Printf("warn: clean attempt local files failed: attempt=%+v err=%v", attempt, err)
			continue
		}
		delete(impl.UncleanedMapHangAttempts, attempt)
	}
}

func (impl *schedulerImpl) cleanAttemptLocalFiles(attempt entity.TaskAttemptKey) error {
	attemptDir, ok := impl.attemptLocalDataDir(attempt)
	if !ok {
		return fmt.Errorf("invalid attempt local data dir: data_dir=%q attempt=%+v", impl.Cfg.DataDir, attempt)
	}
	if err := os.RemoveAll(attemptDir); err != nil {
		return err
	}
	return nil
}

func (impl *schedulerImpl) attemptLocalDataDir(attempt entity.TaskAttemptKey) (string, bool) {
	if impl.Cfg.DataDir == "" ||
		!safePathSegment(attempt.JobID) ||
		!safePathSegment(attempt.TaskID) ||
		!safePathSegment(attempt.AttemptID) {
		return "", false
	}
	return impl.attemptLocalDataDirFromParts(attempt.JobID, attempt.TaskID, attempt.AttemptID)
}

func (impl *schedulerImpl) attemptLocalDataDirFromParts(jobID string, taskID string, attemptID string) (string, bool) {
	if impl.Cfg.DataDir == "" ||
		!safePathSegment(jobID) ||
		!safePathSegment(taskID) ||
		!safePathSegment(attemptID) {
		return "", false
	}
	dataDir := filepath.Clean(impl.Cfg.DataDir)
	attemptDir := filepath.Clean(filepath.Join(dataDir, jobID, taskID, attemptID))
	taskDir := filepath.Clean(filepath.Join(dataDir, jobID, taskID))
	if filepath.Dir(attemptDir) != taskDir {
		return "", false
	}
	return attemptDir, true
}

func safePathSegment(segment string) bool {
	return segment != "" && filepath.Base(segment) == segment
}
