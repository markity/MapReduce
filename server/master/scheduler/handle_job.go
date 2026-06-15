package scheduler

import (
	"mapreduce/server/master/entity"
	"mapreduce/tool"
	"strconv"
	"time"
)

func (impl *schedulerImpl) canCleanupJobIntermediate(jobID string) bool {
	job := impl.jobStatus[jobID]
	return job != nil && job.isTerminal()
}

func (impl *schedulerImpl) refreshJobStage(job *jobStatus) {
	if len(job.AllMapTasks) == 0 && len(job.AllReduceTasks) == 0 {
		impl.refreshRunnableJob(job)
		return
	}

	allMapsDone := job.allMapTasksDone()
	allReducesDone := job.allReduceTasksDone()

	if allReducesDone {
		job.JobStage = entity.JobStageCodeSucceeded
		impl.refreshRunnableJob(job)
		return
	}
	if job.JobStage == entity.JobStageCodeReducing || allMapsDone {
		job.JobStage = entity.JobStageCodeReducing
		impl.refreshRunnableJob(job)
		return
	}
	job.JobStage = entity.JobStageCodeMapping
	impl.refreshRunnableJob(job)
}

func (impl *schedulerImpl) handleReduceFetchFailure(report entity.TaskReport) {
	var job *jobStatus = impl.jobStatus[report.JobID]
	if job == nil {
		return
	}
	var reduceTask *taskStatus = job.AllReduceTasks[report.TaskID]
	if reduceTask == nil {
		return
	}

	reduceTask.Error = report.Error

	for _, lostOutput := range report.LostMapOutputs {
		job.removeMapOutputMeta(lostOutput.TaskID, lostOutput.AttemptID)
		mapTask := job.AllMapTasks[lostOutput.TaskID]
		if mapTask == nil {
			continue
		}
		if len(job.AllAvailableMapOutputs[lostOutput.TaskID]) == 0 &&
			len(job.InFlightMapTasks[lostOutput.TaskID]) == 0 {
			job.markTaskPending(mapTask, "map output lost")
		}
	}

	job.JobStage = entity.JobStageCodeReducing
	impl.refreshRunnableJob(job)
}

func (impl *schedulerImpl) releaseReportedTaskFromWorker(workerUniqueID string, key entity.TaskAttemptKey) {
	worker := impl.workerStatus[workerUniqueID]
	if worker == nil {
		return
	}
	worker.releaseTask(key)
}

func (impl *schedulerImpl) handleCreateMapReduceJobInput(input *createMapReduceJobInput) {
	var pluginFilePath string
	plugin := impl.pluginStatus[input.Req.PluginUniqueID]
	if !pluginVisible(plugin) {
		input.C <- createPluginNotFoundJobResp()
		return
	}
	pluginFilePath, ok := tool.JoinPathPath(impl.pluginStorePath, plugin.PluginUniqueID)
	if !ok {
		panic("unexpected")
	}
	splits := input.Req.TaskSplits
	if len(splits) == 0 {
		input.C <- &entity.CreateMapReduceJobOutput{Code: entity.CreateMapReduceJobCodeInternalError}
		return
	}

	jobID := makeJobID(impl.nextJobSeq)
	impl.nextJobSeq++
	now := time.Now()
	job := &jobStatus{
		JobID:                  jobID,
		JobName:                input.Req.JobName,
		PluginUniqueID:         input.Req.PluginUniqueID,
		PluginFilePath:         pluginFilePath,
		Conf:                   cloneStringMap(input.Req.Conf),
		NumReduceTasks:         input.Req.NumReduceTasks,
		JobStage:               entity.JobStageCodeMapping,
		CreatedAt:              now,
		UpdatedAt:              now,
		AllMapTasks:            make(map[string]*taskStatus),
		AllAvailableMapOutputs: make(map[string][]entity.MapOutputMetaEntry),
		PendingMapTasks:        make(map[string]struct{}),
		InFlightMapTasks:       make(map[string]map[entity.TaskAttemptKey]struct{}),
		AllReduceTasks:         make(map[string]*taskStatus),
		PendingReduceTasks:     make(map[string]struct{}),
		InFlightReduceTasks:    make(map[string]map[entity.TaskAttemptKey]struct{}),
		AckedReports:           make(map[entity.TaskAttemptKey]struct{}),
		LastError:              "",
	}
	impl.jobStatus[jobID] = job
	impl.materializeJobTasks(job, splits)
	impl.refreshRunnableJob(job)

	input.C <- &entity.CreateMapReduceJobOutput{
		Code:  entity.CreateMapReduceJobCodeOK,
		JobID: jobID,
	}
}

func (impl *schedulerImpl) materializeJobTasks(job *jobStatus, splits []entity.SplitSpec) {
	for idx, split := range splits {
		taskID := "map-" + strconv.Itoa(idx)
		job.AllMapTasks[taskID] = &taskStatus{
			TaskType:  entity.TaskTypeMap,
			JobID:     job.JobID,
			TaskID:    taskID,
			TaskState: TaskRunStatePending,
			Error:     "",
			MapTaskSpec: &entity.MapTaskSpec{
				NumReduce: job.NumReduceTasks,
				Split:     split,
			},
		}
		job.AllAvailableMapOutputs[taskID] = make([]entity.MapOutputMetaEntry, 0)
		job.PendingMapTasks[taskID] = struct{}{}
	}
	for partitionID := 0; partitionID < job.NumReduceTasks; partitionID++ {
		taskID := "reduce-" + strconv.Itoa(partitionID)
		job.AllReduceTasks[taskID] = &taskStatus{
			TaskType:  entity.TaskTypeReduce,
			JobID:     job.JobID,
			TaskID:    taskID,
			TaskState: TaskRunStatePending,
			Error:     "",
			ReduceTaskSpec: &entity.ReduceTaskSpec{
				ReducePartitionID: partitionID,
				JobID:             job.JobID,
				// pick的时候再写入
				MapOutputs: nil,
			},
		}
		job.PendingReduceTasks[taskID] = struct{}{}
	}
}

func makeJobID(seq uint64) string {
	return "job-" + time.Now().Format("20060102150405") + "-" + strconv.FormatUint(seq, 10)
}

func (impl *schedulerImpl) handleListJobsInput(input *listJobsInput) {
	jobs := make([]entity.JobInfo, 0, len(impl.jobStatus))
	for _, job := range impl.jobStatus {
		jobs = append(jobs, job.toJobInfo())
	}
	input.C <- &entity.ListJobsOutput{
		Code: entity.ListJobsCodeOK,
		Jobs: jobs,
	}
}

func (impl *schedulerImpl) handleGetJobInput(input *getJobInput) {
	job := impl.jobStatus[input.Req.JobID]
	if job == nil {
		input.C <- &entity.GetJobOutput{
			Code: entity.GetJobCodeNotFound,
		}
		return
	}
	input.C <- &entity.GetJobOutput{
		Code: entity.GetJobCodeOK,
		Job:  job.toJobInfoPtr(),
	}
}

func (j *jobStatus) toJobInfo() entity.JobInfo {
	return entity.JobInfo{
		JobID:   j.JobID,
		JobName: j.JobName,
		Status:  j.publicStatus(),
	}
}

func (j *jobStatus) toJobInfoPtr() *entity.JobInfo {
	info := j.toJobInfo()
	return &info
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

type createMapReduceJobInput struct {
	Req *entity.CreateMapReduceJobInput
	C   chan *entity.CreateMapReduceJobOutput
}

func (impl *schedulerImpl) CreateMapReduceJob(req *entity.CreateMapReduceJobInput) *entity.CreateMapReduceJobOutput {
	c := make(chan *entity.CreateMapReduceJobOutput, 1)
	impl.createJobInputChan <- &createMapReduceJobInput{
		Req: req,
		C:   c,
	}
	return <-c
}

type listJobsInput struct {
	C chan *entity.ListJobsOutput
}

func (impl *schedulerImpl) ListJobs() *entity.ListJobsOutput {
	c := make(chan *entity.ListJobsOutput, 1)
	impl.listJobsInputChan <- &listJobsInput{C: c}
	return <-c
}

type getJobInput struct {
	Req *entity.GetJobInput
	C   chan *entity.GetJobOutput
}

func (impl *schedulerImpl) GetJob(req *entity.GetJobInput) *entity.GetJobOutput {
	c := make(chan *entity.GetJobOutput, 1)
	impl.getJobInputChan <- &getJobInput{
		Req: req,
		C:   c,
	}
	return <-c
}
