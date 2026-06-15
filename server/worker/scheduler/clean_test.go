package scheduler

import (
	"mapreduce/server/worker/entity"
	"os"
	"path/filepath"
	"testing"
)

func TestHandleCleanableJobsRemovesJobFilesAndAttempts(t *testing.T) {
	dataDir := t.TempDir()
	attemptDir := filepath.Join(dataDir, "job-1", "map-1", "attempt-1")
	otherAttemptDir := filepath.Join(dataDir, "job-1", "map-1", "attempt-keep")
	if err := os.MkdirAll(attemptDir, 0o755); err != nil {
		t.Fatalf("mkdir attempt dir: %v", err)
	}
	if err := os.MkdirAll(otherAttemptDir, 0o755); err != nil {
		t.Fatalf("mkdir other attempt dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(attemptDir, "map-output"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write attempt file: %v", err)
	}

	impl := &schedulerImpl{
		Cfg: Config{DataDir: dataDir},
		UncleanedMapHangAttempts: map[entity.TaskAttemptKey]struct{}{
			{JobID: "job-1", TaskID: "map-1", AttemptID: "attempt-1"}: {},
			{JobID: "job-2", TaskID: "map-2", AttemptID: "attempt-2"}: {},
		},
	}

	impl.handleCleanableJobs([]string{"job-1"})

	if _, err := os.Stat(attemptDir); !os.IsNotExist(err) {
		t.Fatalf("attempt dir stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(otherAttemptDir); err != nil {
		t.Fatalf("other attempt dir should remain, stat err = %v", err)
	}
	if _, ok := impl.UncleanedMapHangAttempts[entity.TaskAttemptKey{JobID: "job-1", TaskID: "map-1", AttemptID: "attempt-1"}]; ok {
		t.Fatalf("job-1 attempt should be removed from uncleaned attempts")
	}
	if _, ok := impl.UncleanedMapHangAttempts[entity.TaskAttemptKey{JobID: "job-2", TaskID: "map-2", AttemptID: "attempt-2"}]; !ok {
		t.Fatalf("unrelated job-2 attempt should remain")
	}
}
