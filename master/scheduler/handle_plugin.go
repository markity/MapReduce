package scheduler

import (
	"errors"
	"log"
	"mapreduce/master/entity"
	"mapreduce/tool"
	"os"
	"time"
)

type getJobPluginFilePathAndPinInput struct {
	JobID     string
	PinSecret *string
	C         chan GetJobPluginFilePathOutput
}

type unpinJobPluginInput struct {
	JobID     string
	PinSecret string
	C         chan bool
}

type getPluginFilePathAndPinInput struct {
	PluginUniqueID string
	PinSecret      *string
	C              chan GetPluginFilePathOutput
}

type unpinPluginInput struct {
	PluginUniqueID string
	PinSecret      string
	C              chan bool
}

type registerPluginInput struct {
	PluginName string
	C          chan string
}

type cleanupPluginsInput struct {
	C chan error
}

type deletePluginInput struct {
	PluginUniqueID string
	C              chan deletePluginOutput
}

type listPluginsInput struct {
	C chan []PluginSnapshot
}

const maxPluginIDGenerateAttempts = 16

func (impl *schedulerImpl) handleGetJobPluginFilePathInput(input *getJobPluginFilePathAndPinInput) {
	job := impl.jobStatus[input.JobID]
	if job == nil {
		input.C <- GetJobPluginFilePathOutput{Code: GetJobPluginFilePathCodeJobAndPinNotFound}
		return
	}
	if job.isTerminal() {
		input.C <- GetJobPluginFilePathOutput{Code: GetJobPluginFilePathCodeJobAndPinTerminated}
		return
	}

	if input.PinSecret != nil {
		secret := tool.GitLikeRandomHex(32)
		p := impl.pluginStatus[job.PluginUniqueID]
		if p == nil {
			input.C <- GetJobPluginFilePathOutput{Code: GetJobPluginFilePathCodeJobAndPinTerminated}
			return
		}
		*input.PinSecret = secret
		p.Pin[secret] = struct{}{}
	}
	input.C <- GetJobPluginFilePathOutput{Code: GetJobPluginFilePathAndPinCodeOK, FilePath: job.PluginFilePath}
}

func (impl *schedulerImpl) handleUnpinJobPluginInput(input *unpinJobPluginInput) {
	job := impl.jobStatus[input.JobID]
	if job == nil {
		input.C <- false
		return
	}
	plugin := impl.pluginStatus[job.PluginUniqueID]
	if plugin == nil {
		input.C <- false
		return
	}
	_, ok := plugin.Pin[input.PinSecret]
	if ok {
		delete(plugin.Pin, input.PinSecret)
	}
	input.C <- ok
}

func (impl *schedulerImpl) handleGetPluginFilePathInput(input *getPluginFilePathAndPinInput) {
	plugin := impl.pluginStatus[input.PluginUniqueID]
	if !pluginVisible(plugin) {
		input.C <- GetPluginFilePathOutput{Code: GetPluginFilePathCodePluginNotFound}
		return
	}
	if input.PinSecret != nil {
		secret := tool.GitLikeRandomHex(32)
		*input.PinSecret = secret
		plugin.Pin[secret] = struct{}{}
	}
	pluginFilePath, ok := tool.JoinPathPath(impl.pluginStorePath, plugin.PluginUniqueID)
	if !ok {
		input.C <- GetPluginFilePathOutput{Code: GetPluginFilePathCodePluginNotFound}
		log.Println("warn: handleGetPluginFilePathInput: JoinPathPath pluginUniqueID not valid for path: " + pluginFilePath)
		return
	}
	input.C <- GetPluginFilePathOutput{Code: GetPluginFilePathCodeOK, FilePath: pluginFilePath}
}

func (impl *schedulerImpl) handleUnpinPluginInput(input *unpinPluginInput) {
	plugin := impl.pluginStatus[input.PluginUniqueID]
	if plugin == nil {
		input.C <- false
		return
	}
	_, ok := plugin.Pin[input.PinSecret]
	if ok {
		delete(plugin.Pin, input.PinSecret)
	}
	input.C <- ok
}

func (impl *schedulerImpl) handleRegisterPluginInput(input *registerPluginInput) {
	if impl.pluginStatus == nil {
		impl.pluginStatus = make(map[string]*pluginStatus)
	}

	var pluginUniqueID string
	for i := 0; i < maxPluginIDGenerateAttempts; i++ {
		candidate := input.PluginName + "-" + tool.GitLikeRandomHex(32)
		if _, exists := impl.pluginStatus[candidate]; exists {
			continue
		}
		path, ok := tool.JoinPathPath(impl.pluginStorePath, candidate)
		if !ok {
			log.Println("warn: handleRegisterPluginInput: generated plugin unique id is not valid for path: " + candidate)
			continue
		}
		if _, err := os.Stat(path); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Printf("warn: handleRegisterPluginInput: stat plugin path failed: %v", err)
			continue
		}
		pluginUniqueID = candidate
		break
	}
	if pluginUniqueID == "" {
		input.C <- ""
		return
	}
	impl.pluginStatus[pluginUniqueID] = &pluginStatus{
		PluginUniqueID: pluginUniqueID,
		ModTime:        time.Now(),
		DeletedAt:      nil,
		Pin:            make(map[string]struct{}),
	}
	input.C <- pluginUniqueID
}

func (impl *schedulerImpl) handleDeletePluginInput(input *deletePluginInput) {
	plugin := impl.pluginStatus[input.PluginUniqueID]
	if !pluginVisible(plugin) {
		input.C <- deletePluginOutput{}
		return
	}
	now := time.Now()
	// 等于 plugin.FilePath + ".deleted"，标记文件被删除，若delete但是机器重启
	//	内存状态消失，可以用这个文件判断文件是否是被删除的状态
	filePath, ok := tool.JoinPathPath(impl.pluginStorePath, plugin.PluginUniqueID)
	if !ok {
		input.C <- deletePluginOutput{
			Err: errors.New("plugin unique id is not valid for path: " + plugin.PluginUniqueID),
		}
		log.Println("handleDeletePluginInput: plugin unique id is not valid for path: " + plugin.PluginUniqueID)
		return
	}
	if err := os.WriteFile(deletedPluginMarkerPath(filePath), []byte(now.Format(time.RFC3339Nano)), 0o644); err != nil {
		input.C <- deletePluginOutput{Err: err}
		return
	}
	plugin.DeletedAt = &now
	input.C <- deletePluginOutput{Ok: true}
}

func (impl *schedulerImpl) handleListPluginsInput(input *listPluginsInput) {
	plugins := make([]PluginSnapshot, 0, len(impl.pluginStatus))
	for _, plugin := range impl.pluginStatus {
		if pluginVisible(plugin) {
			plugins = append(plugins, pluginSnapshotFromStatus(plugin))
		}
	}
	input.C <- plugins
}

func (impl *schedulerImpl) handleCleanupPluginsInput(input *cleanupPluginsInput) {
	activePluginUniqueIDs := make(map[string]struct{})
	for _, job := range impl.jobStatus {
		if job.PluginUniqueID == "" {
			continue
		}
		if job.isTerminal() {
			if p := impl.pluginStatus[job.PluginUniqueID]; p != nil && len(p.Pin) != 0 {
				activePluginUniqueIDs[job.PluginUniqueID] = struct{}{}
			}
			continue
		}
		activePluginUniqueIDs[job.PluginUniqueID] = struct{}{}
	}

	for pluginUniqueID, plugin := range impl.pluginStatus {
		if plugin.DeletedAt == nil {
			continue
		}
		if _, ok := activePluginUniqueIDs[pluginUniqueID]; ok {
			continue
		}
		filePath, ok := tool.JoinPathPath(impl.pluginStorePath, plugin.PluginUniqueID)
		if !ok {
			log.Println("handleDeletePluginInput: plugin unique id is not valid for path: " + plugin.PluginUniqueID)
			continue
		}
		if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			input.C <- err
			return
		}
		if err := os.Remove(deletedPluginMarkerPath(filePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
			input.C <- err
			return
		}
		delete(impl.pluginStatus, pluginUniqueID)
	}
	input.C <- nil
}

func createPluginNotFoundJobResp() *entity.CreateMapReduceJobOutput {
	return &entity.CreateMapReduceJobOutput{
		Code: entity.CreateMapReduceJobCodePluginNotFound,
	}
}

func (impl *schedulerImpl) RegisterPlugin(pluginName string) string {
	c := make(chan string, 1)
	impl.registerPluginChan <- &registerPluginInput{
		PluginName: pluginName,
		C:          c,
	}
	return <-c
}

type deletePluginOutput struct {
	Ok  bool
	Err error
}

func (impl *schedulerImpl) DeletePlugin(pluginUniqueID string) (bool, error) {
	c := make(chan deletePluginOutput, 1)
	impl.deletePluginChan <- &deletePluginInput{
		PluginUniqueID: pluginUniqueID,
		C:              c,
	}
	out := <-c
	return out.Ok, out.Err
}

func (impl *schedulerImpl) ListPlugins() []PluginSnapshot {
	c := make(chan []PluginSnapshot, 1)
	impl.listPluginsChan <- &listPluginsInput{
		C: c,
	}
	return <-c
}

func (impl *schedulerImpl) CleanupDeletedPlugins() error {
	c := make(chan error, 1)
	impl.cleanupPluginsChan <- &cleanupPluginsInput{C: c}
	return <-c
}

type GetJobPluginFilePathOutput struct {
	FilePath string
	Code     GetJobPluginFilePathAndPinCode
}

type GetPluginFilePathOutput struct {
	FilePath string
	Code     GetPluginFilePathCode
}

type GetJobPluginFilePathAndPinCode int

const (
	GetJobPluginFilePathAndPinCodeOK GetJobPluginFilePathAndPinCode = iota
	GetJobPluginFilePathCodeJobAndPinNotFound
	GetJobPluginFilePathCodeJobAndPinTerminated
)

type GetPluginFilePathCode int

const (
	GetPluginFilePathCodeOK GetPluginFilePathCode = iota
	GetPluginFilePathCodePluginNotFound
)

func (impl *schedulerImpl) GetJobPluginFilePathAndPin(jobID string, pinSecret *string) GetJobPluginFilePathOutput {
	c := make(chan GetJobPluginFilePathOutput, 1)
	impl.getJobPluginFilePathAndPinChan <- &getJobPluginFilePathAndPinInput{
		JobID:     jobID,
		PinSecret: pinSecret,
		C:         c,
	}
	return <-c
}

func (impl *schedulerImpl) JobPluginFileUnpin(jobID string, secret string) bool {
	c := make(chan bool, 1)
	impl.unpinJobPluginChan <- &unpinJobPluginInput{
		JobID:     jobID,
		PinSecret: secret,
		C:         c,
	}
	return <-c
}

func (impl *schedulerImpl) GetPluginFilePathAndPin(pluginUniqueID string, pinSecret *string) GetPluginFilePathOutput {
	c := make(chan GetPluginFilePathOutput, 1)
	impl.getPluginFilePathAndPinChan <- &getPluginFilePathAndPinInput{
		PluginUniqueID: pluginUniqueID,
		PinSecret:      pinSecret,
		C:              c,
	}
	return <-c
}

func (impl *schedulerImpl) PluginFileUnpin(pluginUniqueID string, secret string) bool {
	c := make(chan bool, 1)
	impl.unpinPluginChan <- &unpinPluginInput{
		PluginUniqueID: pluginUniqueID,
		PinSecret:      secret,
		C:              c,
	}
	return <-c
}
