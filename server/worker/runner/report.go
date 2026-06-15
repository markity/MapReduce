package runner

import (
	"encoding/json"
	"os"
	"path/filepath"

	"mapreduce/server/worker/entity"
)

func writeTaskReport(path string, report entity.TaskReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
