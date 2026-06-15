package runner

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	mrplugin "mapreduce/plugin"
	workercall "mapreduce/rpc/master/worker-call"
	"mapreduce/server/worker/entity"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	stdplugin "plugin"
	"sort"
	"strconv"
	"strings"
)

type TaskSpec struct {
	Assign         entity.TaskSlotAssigned `json:"assign"`
	AttemptDir     string                  `json:"attempt_dir"`
	ReportPath     string                  `json:"report_path"`
	MasterAddr     string                  `json:"master_addr"`
	WorkerUniqueID string                  `json:"worker_unique_id"`
	WorkerAddr     string                  `json:"worker_addr"`
}

type loadedPlugin struct {
	Conf   mrplugin.Configuration
	Plugin mrplugin.JobPlugin
}

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

func runMapTask(spec TaskSpec, loaded *loadedPlugin, report entity.TaskReport) entity.TaskReport {
	if spec.Assign.MapTask == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "map task spec is nil"
		return report
	}
	if loaded == nil || loaded.Plugin == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "plugin is not loaded"
		return report
	}
	if err := executeMap(spec, loaded); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	size, err := mapOutputSize(spec.AttemptDir, spec.Assign.MapTask.NumReduce)
	if err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	report.MapOutput = &entity.MapOutputMetaEntry{
		WorkerUniqueID: spec.WorkerUniqueID,
		WorkerAddr:     spec.WorkerAddr,
		TaskAttemptKey: spec.Assign.TaskAttemptKey,
		Size:           size,
	}
	return report
}

func runReduceTask(spec TaskSpec, loaded *loadedPlugin, report entity.TaskReport) entity.TaskReport {
	if spec.Assign.ReduceTask == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "reduce task spec is nil"
		return report
	}
	if lost, err := fetchReduceInputs(spec); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		report.LostMapOutputs = lost
		return report
	}
	if loaded == nil || loaded.Plugin == nil {
		report.Status = entity.TaskReportFailed
		report.Error = "plugin is not loaded"
		return report
	}
	if err := executeReduce(spec, loaded); err != nil {
		report.Status = entity.TaskReportFailed
		report.Error = err.Error()
		return report
	}
	return report
}

func executeMap(spec TaskSpec, loaded *loadedPlugin) error {
	inputFormat := loaded.Plugin.InputFormat()
	if inputFormat == nil {
		return fmt.Errorf("plugin input format is nil")
	}
	factory, ok := inputFormat.(mrplugin.SplitFactory)
	if !ok {
		return fmt.Errorf("input format does not implement plugin.SplitFactory")
	}
	split, err := factory.NewSplit(spec.Assign.MapTask.Split.SplitType)
	if err != nil {
		return err
	}
	if err := split.UnmarshalBinary(spec.Assign.MapTask.Split.Data); err != nil {
		return err
	}
	reader, err := inputFormat.CreateRecordReader(split, loaded.Conf)
	if err != nil {
		return err
	}
	defer reader.Close()
	mapper := loaded.Plugin.Mapper()
	if mapper == nil {
		return fmt.Errorf("plugin mapper is nil")
	}
	ctx, err := newMapContext(spec.AttemptDir, spec.Assign.MapTask.NumReduce, loaded)
	if err != nil {
		return err
	}
	defer ctx.Close()
	for reader.Next() {
		if err := mapper.Map(reader.Key(), reader.Value(), ctx); err != nil {
			return err
		}
	}
	return reader.Err()
}

func executeReduce(spec TaskSpec, loaded *loadedPlugin) error {
	reducer := loaded.Plugin.Reducer()
	if reducer == nil {
		return fmt.Errorf("plugin reducer is nil")
	}
	groups, err := readReduceInputs(filepath.Join(spec.AttemptDir, "reduce-inputs"))
	if err != nil {
		return err
	}
	ctx, err := newReduceContext(filepath.Join(spec.AttemptDir, "reduce-output"), loaded.Conf)
	if err != nil {
		return err
	}
	defer ctx.Close()
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := reducer.Reduce(key, groups[key], ctx); err != nil {
			return err
		}
	}
	return nil
}

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
	_, err = io.Copy(dst, resp.Body)
	return err
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

func loadPlugin(path string, confValues map[string]string) (*loadedPlugin, error) {
	opened, err := stdplugin.Open(path)
	if err != nil {
		return nil, err
	}
	sym, err := opened.Lookup("BuildPlugin")
	if err != nil {
		return nil, err
	}
	build, ok := sym.(func([]string) (mrplugin.Configuration, mrplugin.JobPlugin, error))
	if !ok {
		return nil, fmt.Errorf("BuildPlugin has unexpected signature")
	}
	conf, jobPlugin, err := build(nil)
	if err != nil {
		return nil, err
	}
	if conf == nil {
		conf = mrplugin.NewConfiguration()
	}
	for key, value := range confValues {
		if err := conf.Set(key, value); err != nil {
			return nil, err
		}
	}
	return &loadedPlugin{Conf: conf, Plugin: jobPlugin}, nil
}

func materializePlugin(spec TaskSpec) (string, error) {
	if spec.Assign.Plugin.Type == entity.FromLocalFS && spec.Assign.Plugin.URI != "" {
		if err := verifyPluginFileSHA256(spec.Assign.Plugin.URI, spec.Assign.Plugin.SHA256); err != nil {
			return "", err
		}
		return spec.Assign.Plugin.URI, nil
	}
	pluginUniqueID := spec.Assign.Plugin.PluginUniqueID
	if pluginUniqueID == "" {
		return "", fmt.Errorf("plugin unique id is empty")
	}
	pluginBytes, err := fetchPluginFromMaster(spec.MasterAddr, pluginUniqueID)
	if err != nil {
		return "", err
	}
	if err := verifyBytesSHA256(pluginBytes, spec.Assign.Plugin.SHA256); err != nil {
		return "", err
	}
	pluginPath := filepath.Join(spec.AttemptDir, "plugin.so")
	if err := os.WriteFile(pluginPath, pluginBytes, 0o644); err != nil {
		return "", err
	}
	return pluginPath, nil
}

func fetchPluginFromMaster(masterAddr string, pluginUniqueID string) ([]byte, error) {
	base := strings.TrimRight(masterAddr, "/")
	if base != "" && !strings.Contains(base, "://") {
		base = "http://" + base
	}
	resp, err := http.Get(base + "/client-api/fetch-plugin/" + url.PathEscape(pluginUniqueID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode >= 400 {
		var failure workercall.FetchPluginByJobIDRespOnFailure
		_ = json.Unmarshal(data, &failure)
		if failure.Code != 0 {
			return nil, fmt.Errorf("fetch plugin failed: http=%d code=%d msg=%s", resp.StatusCode, failure.Code, failure.Msg)
		}
		return nil, fmt.Errorf("fetch plugin failed: http=%d", resp.StatusCode)
	}
	return data, nil
}

func verifyPluginFileSHA256(path string, expected string) error {
	if expected == "" {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	actual := fmt.Sprintf("%x", hash.Sum(nil))
	if actual != expected {
		return fmt.Errorf("plugin sha256 mismatch: actual=%s expected=%s", actual, expected)
	}
	return nil
}

func verifyBytesSHA256(data []byte, expected string) error {
	if expected == "" {
		return nil
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != expected {
		return fmt.Errorf("plugin sha256 mismatch: actual=%s expected=%s", actual, expected)
	}
	return nil
}

func mapOutputSize(attemptDir string, numReduce int) (int64, error) {
	var total int64
	for partitionID := 0; partitionID < numReduce; partitionID++ {
		path := filepath.Join(attemptDir, fmt.Sprintf("partition-%d", partitionID))
		info, err := os.Stat(path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

type mapContext struct {
	conf       mrplugin.Configuration
	hashFunc   func([]byte) int64
	partitions []*os.File
}

func newMapContext(attemptDir string, numReduce int, loaded *loadedPlugin) (*mapContext, error) {
	if numReduce <= 0 {
		return nil, fmt.Errorf("num reduce must be positive")
	}
	partitions := make([]*os.File, 0, numReduce)
	for partitionID := 0; partitionID < numReduce; partitionID++ {
		path := filepath.Join(attemptDir, fmt.Sprintf("partition-%d", partitionID))
		file, err := os.Create(path)
		if err != nil {
			for _, opened := range partitions {
				_ = opened.Close()
			}
			return nil, err
		}
		partitions = append(partitions, file)
	}
	hashFunc := loaded.Plugin.HashFunc()
	if hashFunc == nil {
		hashFunc = defaultHash
	}
	return &mapContext{conf: loaded.Conf, hashFunc: hashFunc, partitions: partitions}, nil
}

func (ctx *mapContext) Write(key []byte, value []byte) error {
	partitionID := int(ctx.hashFunc(key) % int64(len(ctx.partitions)))
	if partitionID < 0 {
		partitionID = -partitionID
	}
	_, err := fmt.Fprintf(ctx.partitions[partitionID], "%s\t%s\n", string(key), string(value))
	return err
}

func (ctx *mapContext) Configuration() mrplugin.Configuration {
	return ctx.conf
}

func (ctx *mapContext) Close() error {
	var firstErr error
	for _, file := range ctx.partitions {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type reduceContext struct {
	conf mrplugin.Configuration
	file *os.File
}

func newReduceContext(outputPath string, conf mrplugin.Configuration) (*reduceContext, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	return &reduceContext{conf: conf, file: file}, nil
}

func (ctx *reduceContext) Write(key []byte, value []byte) error {
	_, err := fmt.Fprintf(ctx.file, "%s\t%s\n", string(key), string(value))
	return err
}

func (ctx *reduceContext) Configuration() mrplugin.Configuration {
	return ctx.conf
}

func (ctx *reduceContext) Close() error {
	return ctx.file.Close()
}

func readReduceInputs(inputDir string) (map[string][]string, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, err
	}
	groups := make(map[string][]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := readReduceInputFile(filepath.Join(inputDir, entry.Name()), groups); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

func readReduceInputFile(path string, groups map[string][]string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "\t")
		if !ok {
			continue
		}
		groups[key] = append(groups[key], value)
	}
	return scanner.Err()
}

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
