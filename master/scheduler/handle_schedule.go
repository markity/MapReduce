package scheduler

import (
	"mapreduce/master/entity"
	"strconv"
	"time"
)

func (impl *schedulerImpl) assignTasksToWorker(worker *workerStatus) []entity.AssignedTask {
	assignedTasks := make([]entity.AssignedTask, 0, len(worker.FreeSlots))
	for slotID := range worker.FreeSlots {
		job, task := impl.pickPendingTask()
		if job == nil || task == nil {
			break
		}

		attemptKey := makeTaskAttemptKey(task.JobID, task.TaskID, impl.makeAttemptID())
		job.markAttemptKeyInFlight(attemptKey)
		job.UpdatedAt = time.Now()
		worker.markSlotBusy(slotID, task.TaskType, attemptKey)
		impl.refreshRunnableJob(job)

		assignedTasks = append(assignedTasks, impl.buildAssignedTask(attemptKey, slotID, task))
	}
	return assignedTasks
}

// 每次pick都会选择当前可用的mapOutputs
func (impl *schedulerImpl) pickPendingTask() (*jobStatus, *taskStatus) {
	for {
		job := impl.popRunnableJob()
		if job == nil {
			return nil, nil
		}
		impl.refreshJobStage(job)
		if task := job.pickPendingMapTask(); task != nil {
			return job, task
		}
		if task := job.pickRunnableReduceTask(); task != nil {
			outputs, ok := job.mapOutputsForReduceTask()
			if !ok {
				task.ReduceTaskSpec.MapOutputs = nil
				impl.refreshRunnableJob(job)
				continue
			}
			task.ReduceTaskSpec.MapOutputs = outputs
			return job, task
		}
		impl.refreshRunnableJob(job)
	}
}

func (impl *schedulerImpl) popRunnableJob() *jobStatus {
	for impl.runnableJobQueue.Len() > 0 {
		front := impl.runnableJobQueue.Front()
		jobID, _ := front.Value.(string)
		impl.runnableJobQueue.Remove(front)
		delete(impl.runnableJobIndex, jobID)

		job := impl.jobStatus[jobID]
		if job == nil || !job.hasRunnableTask() {
			continue
		}
		return job
	}
	return nil
}

func (impl *schedulerImpl) refreshRunnableJob(job *jobStatus) {
	if job == nil {
		return
	}
	if job.hasRunnableTask() {
		if _, ok := impl.runnableJobIndex[job.JobID]; ok {
			return
		}
		impl.runnableJobIndex[job.JobID] = impl.runnableJobQueue.PushBack(job.JobID)
		return
	}
	impl.removeRunnableJob(job.JobID)
}

func (impl *schedulerImpl) removeRunnableJob(jobID string) {
	element := impl.runnableJobIndex[jobID]
	if element == nil {
		return
	}
	impl.runnableJobQueue.Remove(element)
	delete(impl.runnableJobIndex, jobID)
}

func (impl *schedulerImpl) makeAttemptID() string {
	impl.nextAttemptSeq++
	return "attempt-" + time.Now().Format("20060102150405") + "-" + strconv.FormatUint(impl.nextAttemptSeq, 10)
}

func (impl *schedulerImpl) buildAssignedTask(taskAttemptKey entity.TaskAttemptKey, slotID string, task *taskStatus) entity.AssignedTask {
	assignedTask := entity.AssignedTask{
		TaskAttemptKey: taskAttemptKey,
		SlotID:         slotID,
		TaskType:       task.TaskType,
	}
	job := impl.jobStatus[task.JobID]
	if job != nil {
		assignedTask.Plugin = entity.PluginSpec{Type: entity.FromMaster}
		assignedTask.Conf = cloneStringMap(job.Conf)
	}
	if task.TaskType == entity.TaskTypeReduce {
		reduceTask := &entity.ReduceTaskSpec{
			ReducePartitionID: reducePartitionIDFromTask(task),
		}
		if task.ReduceTaskSpec != nil {
			reduceTask.ReducePartitionID = task.ReduceTaskSpec.ReducePartitionID
			reduceTask.JobID = task.ReduceTaskSpec.JobID
			reduceTask.MapOutputs = append([]entity.MapOutputMetaEntry(nil), task.ReduceTaskSpec.MapOutputs...)
		}
		assignedTask.ReduceTask = reduceTask
		return assignedTask
	}
	if task.MapTaskSpec != nil {
		assignedTask.MapTask = task.MapTaskSpec
		return assignedTask
	}
	assignedTask.MapTask = &entity.MapTaskSpec{NumReduce: job.NumReduceTasks}
	return assignedTask
}

func reducePartitionIDFromTask(task *taskStatus) int {
	partitionID, err := strconv.Atoi(task.TaskID)
	if err == nil {
		return partitionID
	}
	partitionID, err = strconv.Atoi(trimReduceTaskPrefix(task.TaskID))
	if err == nil {
		return partitionID
	}
	return 0
}

func (j *jobStatus) mapOutputsForReduceTask() ([]entity.MapOutputMetaEntry, bool) {
	result := make([]entity.MapOutputMetaEntry, 0, len(j.AllMapTasks))
	for mapTaskID := range j.AllMapTasks {
		outputs := j.AllAvailableMapOutputs[mapTaskID]
		if len(outputs) == 0 {
			return nil, false
		}
		result = append(result, outputs[0])
	}
	return result, true
}

func trimReduceTaskPrefix(taskID string) string {
	const prefix = "reduce-"
	if len(taskID) < len(prefix) || taskID[:len(prefix)] != prefix {
		return taskID
	}
	return taskID[len(prefix):]
}
