package runner

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"

	"mapreduce/server/worker/entity"
)

func defaultHash(key []byte) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write(key)
	return int64(hash.Sum64() & 0x7fffffffffffffff)
}

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
