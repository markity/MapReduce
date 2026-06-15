package scheduler

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"mapreduce/server/worker/entity"
	"mapreduce/server/worker/spill"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var errMapOutputServerDisabled = errors.New("worker listen addr is empty")

func (impl *schedulerImpl) startMapOutputServer() error {
	if impl.Cfg.ListenAddr == "" {
		return errMapOutputServerDisabled
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/worker-api/map-output/", impl.handleFetchMapOutput)

	listener, err := net.Listen("tcp", impl.Cfg.ListenAddr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("worker map output server failed: %v", err)
		}
	}()
	return nil
}

// Path: /worker-api/map-output/{job_id}/{task_id}/{attempt_id}/{partition_id}
func (impl *schedulerImpl) handleFetchMapOutput(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/worker-api/map-output/"), "/")
	if len(parts) != 4 {
		http.Error(w, "bad map output path", http.StatusBadRequest)
		return
	}

	attempt, partitionID, ok := parseMapOutputRequest(parts[0], parts[1], parts[2], parts[3])
	if !ok {
		http.Error(w, "bad map output params", http.StatusBadRequest)
		return
	}
	if err := impl.serveMapOutputPartition(w, attempt, partitionID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
	}
}

func parseMapOutputRequest(jobID string, taskID string, attemptID string, partitionID string) (entity.TaskAttemptKey, int, bool) {
	if !safePathSegment(jobID) || !safePathSegment(taskID) || !safePathSegment(attemptID) || !safePathSegment(partitionID) {
		return entity.TaskAttemptKey{}, 0, false
	}
	partition, err := strconv.Atoi(partitionID)
	if err != nil || partition < 0 {
		return entity.TaskAttemptKey{}, 0, false
	}
	return entity.TaskAttemptKey{JobID: jobID, TaskID: taskID, AttemptID: attemptID}, partition, true
}

func (impl *schedulerImpl) serveMapOutputPartition(w http.ResponseWriter, attempt entity.TaskAttemptKey, partitionID int) error {
	reader, length, err := impl.openMapOutputPartition(attempt, partitionID)
	if err != nil {
		return err
	}
	defer reader.Close()
	if length == 0 {
		return nil
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.CopyN(w, reader, length)
	return err
}

func (impl *schedulerImpl) openMapOutputPartition(attempt entity.TaskAttemptKey, partitionID int) (io.ReadCloser, int64, error) {
	outputPath, ok := impl.mapOutputPath(attempt)
	if !ok {
		return nil, 0, fmt.Errorf("bad map output attempt")
	}
	index, err := spill.ReadIndex(outputPath+".index", 0)
	if err != nil {
		return nil, 0, err
	}
	if partitionID >= len(index) {
		return nil, 0, fmt.Errorf("partition %d out of range", partitionID)
	}
	item := index[partitionID]
	if item.Length == 0 {
		return io.NopCloser(bytes.NewReader(nil)), 0, nil
	}
	file, err := os.Open(outputPath)
	if err != nil {
		return nil, 0, err
	}
	if _, err := file.Seek(item.Offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	return file, item.Length, nil
}

func (impl *schedulerImpl) mapOutputPath(attempt entity.TaskAttemptKey) (string, bool) {
	attemptDir, ok := impl.attemptLocalDataDir(attempt)
	if !ok {
		return "", false
	}
	return filepath.Join(attemptDir, "map.out"), true
}

var ensureDirMu sync.Mutex

func ensureDir(path string) error {
	ensureDirMu.Lock()
	defer ensureDirMu.Unlock()
	return os.MkdirAll(path, 0o755)
}
