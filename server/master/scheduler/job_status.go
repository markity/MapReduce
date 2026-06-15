package scheduler

import (
	"log"
	"mapreduce/server/master/entity"
	"time"
)

type taskRunState string

const (
	TaskRunStatePending   taskRunState = "pending"
	TaskRunStateRunning   taskRunState = "running"
	TaskRunStateSucceeded taskRunState = "succeeded"
)

type taskStatus struct {
	TaskType       entity.TaskType
	JobID          string
	TaskID         string
	TaskState      taskRunState
	Error          string
	MapTaskSpec    *entity.MapTaskSpec
	ReduceTaskSpec *entity.ReduceTaskSpec
}

type jobStatus struct {
	JobID          string
	JobName        string
	PluginUniqueID string
	PluginFilePath string
	Conf           map[string]string
	NumReduceTasks int
	JobStage       entity.JobStageCode
	CreatedAt      time.Time
	UpdatedAt      time.Time

	AllMapTasks map[string]*taskStatus
	// key=map taskid value=set of output location
	AllAvailableMapOutputs map[string][]entity.MapOutputMetaEntry
	PendingMapTasks        map[string]struct{}
	// map[taskid](set of attempts)
	InFlightMapTasks map[string](map[entity.TaskAttemptKey]struct{})

	AllReduceTasks      map[string]*taskStatus
	PendingReduceTasks  map[string]struct{}
	InFlightReduceTasks map[string](map[entity.TaskAttemptKey]struct{})

	AckedReports map[entity.TaskAttemptKey]struct{}
	LastError    string
}

func makeTaskAttemptKey(jobID string, taskID string, attemptID string) entity.TaskAttemptKey {
	return entity.TaskAttemptKey{
		JobID:     jobID,
		TaskID:    taskID,
		AttemptID: attemptID,
	}
}

func makeTaskAttemptKeyFromTaskStatus(task *taskStatus, attemptID string) entity.TaskAttemptKey {
	return makeTaskAttemptKey(task.JobID, task.TaskID, attemptID)
}

func makeTaskAttemptKeyFromReport(report entity.TaskReport) entity.TaskAttemptKey {
	return makeTaskAttemptKey(report.JobID, report.TaskID, report.AttemptID)
}

func (j *jobStatus) publicStatus() string {
	return string(j.JobStage)
}

func (j *jobStatus) isTerminal() bool {
	return j.JobStage == entity.JobStageCodeSucceeded ||
		j.JobStage == entity.JobStageCodeKilled
}

func (j *jobStatus) hasRunnableTask() bool {
	if j.isTerminal() {
		return false
	}
	if len(j.PendingMapTasks) > 0 {
		return true
	}
	return j.pickRunnableReduceTask() != nil
}

func (j *jobStatus) pickRunnableReduceTask() *taskStatus {
	if j.JobStage != entity.JobStageCodeReducing {
		return nil
	}
	if !j.allMapTasksDone() {
		return nil
	}
	return j.pickTaskByID(j.PendingReduceTasks, j.AllReduceTasks)
}

// 扫描所有的map task的reports，如果每一个task至少有一个成功的report，那么就算all map tasksDone
func (j *jobStatus) allMapTasksDone() bool {
	if len(j.AllMapTasks) == 0 {
		return false
	}
	for taskID := range j.AllMapTasks {
		if !j.hasCompleteMapOutput(taskID) {
			return false
		}
	}

	return true
}

func (j *jobStatus) allReduceTasksDone() bool {
	if len(j.AllReduceTasks) == 0 {
		return false
	}
	for _, reduceTask := range j.AllReduceTasks {
		if reduceTask.TaskState != TaskRunStateSucceeded {
			return false
		}
	}
	return true
}

func (j *jobStatus) markTaskPending(task *taskStatus, err string) bool {
	if task == nil {
		return false
	}
	task.Error = err
	task.TaskState = TaskRunStatePending
	if task.TaskType == entity.TaskTypeMap {
		j.PendingMapTasks[task.TaskID] = struct{}{}
		j.JobStage = entity.JobStageCodeMapping
		return true
	}
	if task.TaskType == entity.TaskTypeReduce {
		j.PendingReduceTasks[task.TaskID] = struct{}{}
		return true
	}
	return false
}

func (j *jobStatus) pickPendingMapTask() *taskStatus {
	return j.pickTaskByID(j.PendingMapTasks, j.AllMapTasks)
}

func (j *jobStatus) pickTaskByID(taskIDs map[string]struct{}, allTasks map[string]*taskStatus) *taskStatus {
	for taskID := range taskIDs {
		task := allTasks[taskID]
		if task == nil {
			delete(taskIDs, taskID)
			continue
		}
		return task
	}
	return nil
}

// 删除inflight
func (job *jobStatus) finishInFlightByAttemptKey(attemptKey entity.TaskAttemptKey) bool {
	_, ok1 := job.InFlightMapTasks[attemptKey.TaskID]
	_, ok2 := job.InFlightReduceTasks[attemptKey.TaskID]
	if ok1 && ok2 {
		panic("check")
	}

	task := job.getTaskByTaskID(attemptKey.TaskID)
	if task == nil {
		return false
	}
	if !job.removeInFlightAttempt(task.TaskType, attemptKey) {
		return false
	}

	return true
}

func (j *jobStatus) removeInFlightAttempt(taskType entity.TaskType, attemptKey entity.TaskAttemptKey) bool {
	switch taskType {
	case entity.TaskTypeMap:
		return removeAttemptFromInFlightTasks(j.InFlightMapTasks, attemptKey)
	case entity.TaskTypeReduce:
		return removeAttemptFromInFlightTasks(j.InFlightReduceTasks, attemptKey)
	default:
		log.Println("warn: removeInFlightAttempt unexpected taskType")
		return false
	}
}

func removeAttemptFromInFlightTasks(
	inFlightTasks map[string]map[entity.TaskAttemptKey]struct{},
	attemptKey entity.TaskAttemptKey,
) bool {
	attempts := inFlightTasks[attemptKey.TaskID]
	if attempts == nil {
		return false
	}
	if _, ok := attempts[attemptKey]; !ok {
		return false
	}
	delete(attempts, attemptKey)
	if len(attempts) == 0 {
		delete(inFlightTasks, attemptKey.TaskID)
	}
	return true
}

func (j *jobStatus) addInFlightAttempt(taskType entity.TaskType, attemptKey entity.TaskAttemptKey) {
	switch taskType {
	case entity.TaskTypeMap:
		addAttemptToInFlightTasks(j.InFlightMapTasks, attemptKey)
	case entity.TaskTypeReduce:
		addAttemptToInFlightTasks(j.InFlightReduceTasks, attemptKey)
	default:
		log.Println("warn: addInFlightAttempt unexpected taskType")
	}
}

func addAttemptToInFlightTasks(
	inFlightTasks map[string]map[entity.TaskAttemptKey]struct{},
	attemptKey entity.TaskAttemptKey,
) {
	attempts := inFlightTasks[attemptKey.TaskID]
	if attempts == nil {
		attempts = make(map[entity.TaskAttemptKey]struct{})
		inFlightTasks[attemptKey.TaskID] = attempts
	}
	attempts[attemptKey] = struct{}{}
}

// 加入inflight队列，并且从pending队列中删除
func (j *jobStatus) markAttemptKeyInFlight(attemptKey entity.TaskAttemptKey) {
	task := j.getTaskByTaskID(attemptKey.TaskID)
	if task == nil {
		panic("check")
	}
	if task.TaskType != entity.TaskTypeMap && task.TaskType != entity.TaskTypeReduce {
		panic("check")
	}
	task.TaskState = TaskRunStateRunning
	if task.TaskType == entity.TaskTypeReduce {
		j.AllReduceTasks[task.TaskID] = task
		delete(j.PendingReduceTasks, task.TaskID)
		j.addInFlightAttempt(task.TaskType, attemptKey)
		return
	}
	j.AllMapTasks[task.TaskID] = task
	delete(j.PendingMapTasks, task.TaskID)
	j.addInFlightAttempt(task.TaskType, attemptKey)
}

func (j *jobStatus) inFlightAttempt(taskType entity.TaskType, key entity.TaskAttemptKey) bool {
	switch taskType {
	case entity.TaskTypeReduce:
		_, ok := j.InFlightReduceTasks[key.TaskID][key]
		if !ok {
			return false
		}
		return true
	case entity.TaskTypeMap:
		_, ok := j.InFlightMapTasks[key.TaskID][key]
		if !ok {
			return false
		}
		return true
	}
	log.Println("warn: inFlightAttempt unexpected taskType")
	return false
}

func (j *jobStatus) hasInFlightAttempt(key entity.TaskAttemptKey) bool {
	if _, ok := j.InFlightMapTasks[key.TaskID][key]; ok {
		return true
	}
	if _, ok := j.InFlightReduceTasks[key.TaskID][key]; ok {
		return true
	}
	return false
}

func (j *jobStatus) clearInFlightAttemptsForTask(task *taskStatus) []entity.TaskAttemptKey {
	if task == nil {
		return nil
	}
	var removed []entity.TaskAttemptKey
	if task.TaskType == entity.TaskTypeMap {
		for key := range j.InFlightMapTasks[task.TaskID] {
			removed = append(removed, key)
		}
		delete(j.InFlightMapTasks, task.TaskID)
		return removed
	}
	if task.TaskType == entity.TaskTypeReduce {
		for key := range j.InFlightReduceTasks[task.TaskID] {
			removed = append(removed, key)
		}
		delete(j.InFlightReduceTasks, task.TaskID)
	}
	return removed
}

func (j *jobStatus) getTaskByTaskID(taskID string) *taskStatus {
	if t, ok := j.AllMapTasks[taskID]; ok {
		return t
	}
	if t, ok := j.AllReduceTasks[taskID]; ok {
		return t
	}
	return nil
}

func (j *jobStatus) removeMapOutputMeta(mapTaskID string, attemptID string) {
	result := make([]entity.MapOutputMetaEntry, 0)
	for _, output := range j.AllAvailableMapOutputs[mapTaskID] {
		if output.AttemptID == attemptID {
			continue
		}
		result = append(result, output)
	}
	j.AllAvailableMapOutputs[mapTaskID] = result
}

func (j *jobStatus) hasCompleteMapOutput(mapTaskID string) bool {
	return len(j.AllAvailableMapOutputs[mapTaskID]) > 0
}

func (j *jobStatus) validateMapSuccessReport(report entity.TaskReport) bool {
	if report.TaskType != entity.TaskTypeMap || report.Status != entity.TaskReportSucceeded {
		return true
	}
	if report.MapOutput == nil {
		return false
	}
	return report.MapOutput.TaskAttemptKey == report.Key()
}

// func (j *jobStatus) ensureTaskIndexes() {
// 	if j.AllMapTasks == nil {
// 		j.AllMapTasks = make(map[string]*taskStatus)
// 	}
// 	if j.PendingMapTasks == nil {
// 		j.PendingMapTasks = make(map[string]struct{})
// 	}
// 	if j.InFlightMapTasks == nil {
// 		j.InFlightMapTasks = make(map[string](map[entity.TaskAttemptKey]struct{}))
// 	}
// 	if j.AllReduceTasks == nil {
// 		j.AllReduceTasks = make(map[string]*taskStatus)
// 	}
// 	if j.PendingReduceTasks == nil {
// 		j.PendingReduceTasks = make(map[string]struct{})
// 	}
// 	if j.InFlightReduceTasks == nil {
// 		j.InFlightReduceTasks = make(map[string](map[entity.TaskAttemptKey]struct{}))
// 	}
// }
