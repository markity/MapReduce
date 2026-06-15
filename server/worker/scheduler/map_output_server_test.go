package scheduler

import (
	"io"
	"mapreduce/server/worker/entity"
	"mapreduce/server/worker/spill"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenMapOutputPartitionReturnsRequestedRange(t *testing.T) {
	dataDir := t.TempDir()
	attempt := entity.TaskAttemptKey{
		JobID:     "job-1",
		TaskID:    "map-1",
		AttemptID: "attempt-1",
	}
	attemptDir := filepath.Join(dataDir, attempt.JobID, attempt.TaskID, attempt.AttemptID)
	if err := os.MkdirAll(attemptDir, 0o755); err != nil {
		t.Fatalf("mkdir attempt dir: %v", err)
	}
	outputPath := filepath.Join(attemptDir, "map.out")
	if err := os.WriteFile(outputPath, []byte("aaabbbbcc"), 0o644); err != nil {
		t.Fatalf("write map output: %v", err)
	}
	if err := spill.WriteIndex(outputPath+".index", []spill.PartitionIndex{
		{Offset: 0, Length: 3},
		{Offset: 3, Length: 4},
		{Offset: 7, Length: 2},
	}); err != nil {
		t.Fatalf("write index: %v", err)
	}

	impl := &schedulerImpl{Cfg: Config{DataDir: dataDir}}
	reader, length, err := impl.openMapOutputPartition(attempt, 1)
	if err != nil {
		t.Fatalf("open partition: %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(io.LimitReader(reader, length))
	if err != nil {
		t.Fatalf("read partition: %v", err)
	}
	if string(got) != "bbbb" {
		t.Fatalf("partition bytes = %q, want %q", got, "bbbb")
	}
}

func TestOpenMapOutputPartitionReturnsEmptyReaderForEmptyPartition(t *testing.T) {
	dataDir := t.TempDir()
	attempt := entity.TaskAttemptKey{
		JobID:     "job-1",
		TaskID:    "map-1",
		AttemptID: "attempt-1",
	}
	attemptDir := filepath.Join(dataDir, attempt.JobID, attempt.TaskID, attempt.AttemptID)
	if err := os.MkdirAll(attemptDir, 0o755); err != nil {
		t.Fatalf("mkdir attempt dir: %v", err)
	}
	outputPath := filepath.Join(attemptDir, "map.out")
	if err := os.WriteFile(outputPath, []byte("aaa"), 0o644); err != nil {
		t.Fatalf("write map output: %v", err)
	}
	if err := spill.WriteIndex(outputPath+".index", []spill.PartitionIndex{
		{Offset: 0, Length: 3},
		{Offset: -1, Length: 0},
	}); err != nil {
		t.Fatalf("write index: %v", err)
	}

	impl := &schedulerImpl{Cfg: Config{DataDir: dataDir}}
	reader, length, err := impl.openMapOutputPartition(attempt, 1)
	if err != nil {
		t.Fatalf("open partition: %v", err)
	}
	defer reader.Close()
	if length != 0 {
		t.Fatalf("length = %d, want 0", length)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read empty partition: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty partition bytes = %q, want empty", got)
	}
}
