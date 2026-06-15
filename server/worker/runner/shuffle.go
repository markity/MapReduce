package runner

import (
	"fmt"
	"io"
	"mapreduce/server/worker/entity"
	"mapreduce/server/worker/spill"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func fetchReduceInputs(spec TaskSpec) ([]entity.MapOutputMetaEntry, error) {
	inputDir := filepath.Join(spec.AttemptDir, "reduce-inputs")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		return nil, err
	}
	lost := make([]entity.MapOutputMetaEntry, 0)
	for _, output := range spec.Assign.ReduceTask.MapOutputs {
		if err := fetchOneMapOutputPartition(output, spec.Assign.ReduceTask.ReducePartitionID, inputDir); err != nil {
			lost = append(lost, output)
		}
	}
	if len(lost) > 0 {
		return lost, fmt.Errorf("failed to fetch %d map output partitions", len(lost))
	}
	return nil, nil
}

func fetchOneMapOutputPartition(output entity.MapOutputMetaEntry, partitionID int, inputDir string) error {
	endpoint := mapOutputURL(output.WorkerAddr, output.JobID, output.TaskID, output.AttemptID, partitionID)
	resp, err := http.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch map output failed: http=%d", resp.StatusCode)
	}
	dstPath := filepath.Join(inputDir, output.TaskID+"-"+output.AttemptID+"-"+strconv.Itoa(partitionID))
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	n, err := io.Copy(dst, resp.Body)
	if err != nil {
		return err
	}
	index := []spill.PartitionIndex{{Offset: -1}}
	if n > 0 {
		index[0] = spill.PartitionIndex{Offset: 0, Length: n}
	}
	return spill.WriteIndex(dstPath+".index", index)
}

func mapOutputURL(workerAddr string, jobID string, taskID string, attemptID string, partitionID int) string {
	base := strings.TrimRight(workerAddr, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return base + "/worker-api/map-output/" +
		url.PathEscape(jobID) + "/" +
		url.PathEscape(taskID) + "/" +
		url.PathEscape(attemptID) + "/" +
		url.PathEscape(strconv.Itoa(partitionID))
}
