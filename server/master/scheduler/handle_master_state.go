package scheduler

import (
	"mapreduce/server/master/entity"
	"sort"
)

type getMasterStateInput struct {
	C chan *entity.GetMasterStateOutput
}

func (impl *schedulerImpl) handleGetMasterStateInput(input *getMasterStateInput) {
	workers := make([]entity.MasterStateWorkerInfo, 0, len(impl.workerStatus))
	for _, worker := range impl.workerStatus {
		workers = append(workers, masterStateWorkerInfoFromStatus(worker))
	}
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].WorkerUniqueID < workers[j].WorkerUniqueID
	})

	plugins := make([]entity.MasterStatePluginInfo, 0, len(impl.pluginStatus))
	for _, plugin := range impl.pluginStatus {
		if !pluginVisible(plugin) {
			continue
		}
		plugins = append(plugins, entity.MasterStatePluginInfo{
			PluginUniqueID: plugin.PluginUniqueID,
			ModTime:        plugin.ModTime,
			PinCount:       len(plugin.Pin),
		})
	}
	sort.Slice(plugins, func(i, j int) bool {
		return plugins[i].PluginUniqueID < plugins[j].PluginUniqueID
	})

	jobs := make([]entity.MasterStateJobInfo, 0, len(impl.jobStatus))
	for _, job := range impl.jobStatus {
		jobs = append(jobs, entity.MasterStateJobInfo{
			JobID:     job.JobID,
			JobName:   job.JobName,
			Status:    job.publicStatus(),
			StartedAt: job.CreatedAt,
		})
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})

	input.C <- &entity.GetMasterStateOutput{
		Code:    entity.GetMasterStateCodeOK,
		Workers: workers,
		Plugins: plugins,
		Jobs:    jobs,
	}
}

func (impl *schedulerImpl) GetMasterState() *entity.GetMasterStateOutput {
	c := make(chan *entity.GetMasterStateOutput, 1)
	impl.getMasterStateChan <- &getMasterStateInput{C: c}
	return <-c
}

func masterStateWorkerInfoFromStatus(worker *workerStatus) entity.MasterStateWorkerInfo {
	slots := make([]entity.MasterStateWorkerSlotInfo, 0, len(worker.AllSlots))
	for _, slot := range worker.AllSlots {
		slots = append(slots, entity.MasterStateWorkerSlotInfo{
			SlotID:             slot.SlotID,
			CurrentRunningTask: taskSlotAssignedToEntity(slot.CurrentRunningTask),
		})
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i].SlotID < slots[j].SlotID
	})

	freeSlots := stringSetKeys(worker.FreeSlots)
	busySlots := stringSetKeys(worker.BusySlots)

	return entity.MasterStateWorkerInfo{
		WorkerUniqueID: worker.UniqueID,
		WorkerAddr:     worker.Addr,
		WorkerEpoch:    worker.Epoch,
		WorkerState:    string(worker.State),
		LastSeen:       worker.LastSeen,
		LastSeq:        worker.LastSeq,
		HasSeq:         worker.HasSeq,
		Slots:          slots,
		FreeSlots:      freeSlots,
		BusySlots:      busySlots,
	}
}

func taskSlotAssignedToEntity(task *taskSlotAssigned) *entity.TaskSlotAssigned {
	if task == nil {
		return nil
	}
	return &entity.TaskSlotAssigned{
		TaskAttemptKey: task.TaskAttemptKey,
		TaskType:       task.TaskType,
	}
}

func stringSetKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
