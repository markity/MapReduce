package scheduler

import (
	"errors"
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

type registerPluginInput struct {
	PluginUniqueID string
	FilePath       string
	C              chan bool
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

func (impl *schedulerImpl) handleRegisterPluginInput(input *registerPluginInput) {
	if impl.pluginStatus == nil {
		impl.pluginStatus = make(map[string]*pluginStatus)
	}
	info, err := os.Stat(input.FilePath)
	if err != nil {
		input.C <- false
		return
	}
	impl.pluginStatus[input.PluginUniqueID] = &pluginStatus{
		PluginUniqueID: input.PluginUniqueID,
		FilePath:       input.FilePath,
		ModTime:        info.ModTime(),
		DeletedAt:      nil,
		Pin:            make(map[string]struct{}),
	}
	input.C <- true
}

func (impl *schedulerImpl) handleDeletePluginInput(input *deletePluginInput) {
	plugin := impl.pluginStatus[input.PluginUniqueID]
	if plugin == nil {
		input.C <- deletePluginOutput{}
		return
	}
	if plugin.DeletedAt == nil {
		now := time.Now()
		if err := os.WriteFile(deletedPluginMarkerPath(plugin.FilePath), []byte(now.Format(time.RFC3339Nano)), 0o644); err != nil {
			input.C <- deletePluginOutput{Err: err}
			return
		}
		plugin.DeletedAt = &now
	}
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
		if err := os.Remove(plugin.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			input.C <- err
			return
		}
		if err := os.Remove(deletedPluginMarkerPath(plugin.FilePath)); err != nil && !errors.Is(err, os.ErrNotExist) {
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

func (impl *schedulerImpl) RegisterPlugin(pluginUniqueID string, filePath string) bool {
	c := make(chan bool, 1)
	impl.registerPluginChan <- &registerPluginInput{
		PluginUniqueID: pluginUniqueID,
		FilePath:       filePath,
		C:              c,
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

type GetJobPluginFilePathAndPinCode int

const (
	GetJobPluginFilePathAndPinCodeOK GetJobPluginFilePathAndPinCode = iota
	GetJobPluginFilePathCodeJobAndPinNotFound
	GetJobPluginFilePathCodeJobAndPinTerminated
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
