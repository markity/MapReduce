package scheduler

import (
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

	outputPath, ok := impl.mapOutputPartitionPath(parts[0], parts[1], parts[2], parts[3])
	if !ok {
		http.Error(w, "bad map output params", http.StatusBadRequest)
		return
	}
	http.ServeFile(w, r, outputPath)
}

func (impl *schedulerImpl) mapOutputPartitionPath(jobID string, taskID string, attemptID string, partitionID string) (string, bool) {
	if !safePathSegment(jobID) || !safePathSegment(taskID) || !safePathSegment(attemptID) || !safePathSegment(partitionID) {
		return "", false
	}
	attemptDir, ok := impl.attemptLocalDataDirFromParts(jobID, taskID, attemptID)
	if !ok {
		return "", false
	}
	return filepath.Join(attemptDir, mapOutputPartitionFileName(partitionID)), true
}

func mapOutputPartitionFileName(partitionID string) string {
	return "partition-" + partitionID
}

var ensureDirMu sync.Mutex

func ensureDir(path string) error {
	ensureDirMu.Lock()
	defer ensureDirMu.Unlock()
	return os.MkdirAll(path, 0o755)
}
