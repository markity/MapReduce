package scheduler

import (
	"fmt"
	"log"
	"mapreduce/server/master/entity"
	"time"
)

// 从心跳中创建一个新的worker, 此函数在两个场景调用
//
//  1. 以前没有见过该WorkerUniqueID，创建一个新的
//  2. 以前见过，但是epoch变大了，也就是机发生了重启，此时调用方先delete，再调用handleUnknownWorker
func (impl *schedulerImpl) handleUnknownWorker(heartbeatReq *entity.HeartbeatInput) {
	if impl.workerStatus[heartbeatReq.WorkerUniqueID] != nil {
		panic("check")
	}
	impl.workerStatus[heartbeatReq.WorkerUniqueID] = newWorkerStatusFromHeartbeat(
		heartbeatReq.WorkerUniqueID,
		heartbeatReq.WorkerAddr,
		heartbeatReq.WorkerEpoch,
	)
}

// heartbeat seq校验ok后，先处理reports，再对齐两方slots状态，进行任务重新调度
func (impl *schedulerImpl) updateWorkerSlotSnapshot(worker *workerStatus, slots map[string]entity.TaskSlotStatus) (shouldStopAttempts []entity.TaskAttemptKey) {
	shouldStopAttempts = impl.requeueLostRunningTasks(worker, slots)
	worker.rebuildSlotSnapshot(slots)
	return shouldStopAttempts
}

// heartbeat可能持有之前workerStatus不存在的slots，比如worker扩容，此时只注册slot。
// 是否free/busy必须以本次真实上报的snapshot为准，不能在这里提前放入FreeSlots。
func (impl *schedulerImpl) workerExpandSlotsByReportedSlots(worker *workerStatus, reportedSlots map[string]entity.TaskSlotStatus) {
	for _, reported := range reportedSlots {
		if reported.SlotID == "" {
			continue
		}
		worker.ensureSlot(reported.SlotID)
	}
}

// diff重新调度任务, 返回需要停止的attempt
// 对比本地worker slots状态，以及heartbeat上报的slots状态，重新调度任务
func (impl *schedulerImpl) requeueLostRunningTasks(
	worker *workerStatus,
	reportedSlots map[string]entity.TaskSlotStatus,
) (shouldStopAttempts []entity.TaskAttemptKey) {
	shouldStopAttempts = make([]entity.TaskAttemptKey, 0)

	for workerSlotID, oldSlot := range worker.AllSlots {
		reportedSlot, ok := reportedSlots[workerSlotID]
		if !ok {
			reportedSlot = entity.TaskSlotStatus{}
		}

		oldTask := oldSlot.CurrentRunningTask
		reportedTask := reportedSlot.CurrentRunningTask

		// master 之前认为这个 slot 有任务在跑，
		// 但 worker 最新 slot snapshot 里没有这个 attempt。
		// 说明旧 attempt 已经不可信，需要重新调度。
		if oldTask != nil {
			oldAttempt := oldTask.TaskAttemptKey

			if reportedTask == nil || reportedTask.TaskAttemptKey != oldAttempt {
				job := impl.jobStatus[oldAttempt.JobID]
				impl.rescheduleLostAttempt(oldAttempt)
				// 这个函数相当于重新调度，重新将job放入runnable
				impl.refreshRunnableJob(job)
			}
		}

		// worker 报告自己正在跑一个 attempt，
		// 但 master 不认为这个 slot 上应该跑它。
		// 说明这是 stale / unknown / duplicated attempt，需要通知 worker 停掉。
		if reportedTask != nil {
			reportedAttempt := reportedTask.TaskAttemptKey

			if oldTask == nil || oldTask.TaskAttemptKey != reportedAttempt {
				shouldStopAttempts = append(shouldStopAttempts, reportedAttempt)
				continue
			}
			job := impl.jobStatus[reportedAttempt.JobID]
			if job == nil || !job.hasInFlightAttempt(reportedAttempt) {
				shouldStopAttempts = append(shouldStopAttempts, reportedAttempt)
			}
		}
	}

	return shouldStopAttempts
}

// epoch变大，说明机器重启，此时收到心跳服务端应该直接尝试重新调度在这个worker上的所有task
//
//	调用它前worker slots和req slots都已经expand
func (impl *schedulerImpl) handleWorkerRestart(worker *workerStatus, heartbeatReq *entity.HeartbeatInput) {
	impl.requeueAllWorkerRunningAttempts(worker)

	// 复用handleUnknownWorker逻辑
	delete(impl.workerStatus, worker.UniqueID)
	impl.handleUnknownWorker(heartbeatReq)

	// 初始化slot，全都标记free
	for reportedSlotID, _ := range heartbeatReq.SlotStatus {
		impl.workerStatus[worker.UniqueID].markSlotFree(reportedSlotID)
	}
}

func (impl *schedulerImpl) requeueAllWorkerRunningAttempts(worker *workerStatus) {
	for _, slot := range worker.AllSlots {
		if slot.CurrentRunningTask == nil {
			continue
		}
		attempt := slot.CurrentRunningTask.TaskAttemptKey
		job := impl.jobStatus[attempt.JobID]
		impl.rescheduleLostAttempt(attempt)
		impl.refreshRunnableJob(job)
	}
}

func (impl *schedulerImpl) rescheduleByReport(report entity.TaskReport) {
	job := impl.jobStatus[report.JobID]
	if job == nil {
		return
	}

	task := job.getTaskByTaskID(report.TaskID)
	if task == nil {
		return
	}
	if task.TaskType != entity.TaskTypeMap && task.TaskType != entity.TaskTypeReduce {
		log.Println("warn: handleTaskReportIdempotently: task type unexpected: " + fmt.Sprint(task.TaskType))
	}

	if report.Status == entity.TaskReportSucceeded {
		if task.TaskType == entity.TaskTypeMap {
			delete(job.PendingMapTasks, report.TaskID)
			task.TaskState = TaskRunStateSucceeded
			job.clearInFlightAttemptsForTask(task)
			job.AllAvailableMapOutputs[report.TaskID] = append(job.AllAvailableMapOutputs[report.TaskID], *report.MapOutput)
		} else {
			delete(job.PendingReduceTasks, report.TaskID)
			job.AllReduceTasks[report.TaskID].TaskState = TaskRunStateSucceeded
			job.clearInFlightAttemptsForTask(task)
		}
	} else if report.Status == entity.TaskReportFailed {
		if task.TaskType == entity.TaskTypeMap {
			if len(job.AllAvailableMapOutputs[report.TaskID]) == 0 &&
				len(job.InFlightMapTasks[report.TaskID]) == 0 {
				job.markTaskPending(task, report.Error)
			}
		} else {
			if task.TaskState == TaskRunStateSucceeded {
				return
			} else {
				if len(job.InFlightReduceTasks[report.TaskID]) == 0 {
					job.markTaskPending(task, report.Error)
				}
			}
		}
	}
}

func (impl *schedulerImpl) handleTaskReportIdempotently(workerUniqueID string, report entity.TaskReport) bool {
	job := impl.jobStatus[report.JobID]
	if job == nil {
		return true
	}

	taskType := report.TaskType

	// 保证一个attempt key最多只处理一次，这里有一个set记录
	if _, ok := job.AckedReports[report.Key()]; ok {
		return true
	}

	ok := job.inFlightAttempt(taskType, makeTaskAttemptKeyFromReport(report))
	if !ok {
		job.AckedReports[report.Key()] = struct{}{}
		return true
	}
	if !job.validateMapSuccessReport(report) {
		report.Status = entity.TaskReportFailed
		report.Error = "invalid map output report"
		report.MapOutput = nil
	}
	job.AckedReports[report.Key()] = struct{}{}

	ok = job.finishInFlightByAttemptKey(report.Key())
	if !ok {
		return true
	}

	// 删除无法使用的mapOutputs
	if report.TaskType == entity.TaskTypeReduce && report.Status != entity.TaskReportSucceeded {
		impl.handleReduceFetchFailure(report)
	}

	// pending相关
	impl.rescheduleByReport(report)

	// 释放对应的槽
	impl.releaseReportedTaskFromWorker(workerUniqueID, report.Key())
	job.UpdatedAt = time.Now()
	impl.refreshJobStage(job)
	return true
}

func (impl *schedulerImpl) rescheduleLostAttempt(attempt entity.TaskAttemptKey) {
	job, ok := impl.jobStatus[attempt.JobID]
	if !ok {
		return
	}

	task := job.getTaskByTaskID(attempt.TaskID)
	if task == nil {
		return
	}

	if task.TaskType != entity.TaskTypeMap && task.TaskType != entity.TaskTypeReduce {
		log.Println("warn: rescheduleLostAttempt: task type invalid: " + fmt.Sprint(task.TaskType))
	}

	if task.TaskType == entity.TaskTypeMap {
		job.removeInFlightAttempt(task.TaskType, attempt)
		if len(job.AllAvailableMapOutputs[task.TaskID]) == 0 &&
			len(job.InFlightMapTasks[attempt.TaskID]) == 0 {
			job.markTaskPending(task, "attempt lost")
			impl.refreshJobStage(job)
		}
	} else {
		job.removeInFlightAttempt(task.TaskType, attempt)
		if task.TaskState != TaskRunStateSucceeded &&
			len(job.InFlightReduceTasks[attempt.TaskID]) == 0 {
			job.markTaskPending(task, "attempt lost")
		}
	}
}

type heartbeatReqInput struct {
	Req *entity.HeartbeatInput
	C   chan *entity.HeartbeatOutput
}

func (impl *schedulerImpl) PostHeartbeatReq(req *entity.HeartbeatInput) *entity.HeartbeatOutput {
	c := make(chan *entity.HeartbeatOutput, 1)
	impl.heartbeatReqInputChan <- &heartbeatReqInput{
		Req: req,
		C:   c,
	}
	return <-c
}

func (impl *schedulerImpl) getOrCreateWorker(req *entity.HeartbeatInput) *workerStatus {
	worker, ok := impl.workerStatus[req.WorkerUniqueID]
	if !ok {
		// 找不到workerstatus, 说明是新机器, 加入到impl.workerStatus映射中, 初始状态为alive
		//	此时的workerstatus[workerUniqueID].HasSeq == false
		impl.handleUnknownWorker(req)
		worker = impl.workerStatus[req.WorkerUniqueID]
	}
	return worker
}

// input: worker上报心跳维护存活并且上报信息, 包括: 任务状态, 机器资源槽资源
// output: master发配任务, 告知希望知道的job运行状态(便于在job结束时清空map任务产生的临时资源), ack客户端的上报信息
func (impl *schedulerImpl) handleHeartbeatInput(heartbeatInput *heartbeatReqInput) {
	respOut := entity.HeartbeatOutput{
		AckedReports:       make([]entity.TaskAttemptKey, 0),
		CleanableJobs:      make([]string, 0),
		AssignedTasks:      make([]entity.AssignedTask, 0),
		ShouldStopAttempts: make([]entity.TaskAttemptKey, 0),
	}
	var codeOut entity.HeartbeatOutputCode
	defer func() {
		respOut.Code = codeOut
		heartbeatInput.C <- &respOut
	}()

	req := heartbeatInput.Req
	// 若worker不存在，创建一个空worker，状态alive，其它的所有状态为空，包括slots信息
	// 	lastSeen=now，state=alive。uid，addr，epoch随req参数一样。其它信息是默认值，引用类型都make了，不会空指针
	var worker *workerStatus = impl.getOrCreateWorker(req)

	// 收到epoch小于记录值的，很有可能是两个uniqueid一样的worker在跑, 警戒一下
	//	当然也有可能收到滞留很久的input，但概率不大
	if req.WorkerEpoch < worker.Epoch {
		log.Println("warn: worker epoch stale, maybe two worker running at the same time")
		codeOut = entity.HeartbeatOutputCodeEpochStaleRequest
		return
	}

	// epoch大于记录值，确信发生了宕机
	if req.WorkerEpoch > worker.Epoch {
		// 可能上报新槽，这里扩展worker的slot槽位
		impl.workerExpandSlotsByReportedSlots(worker, req.SlotStatus)

		// 处理宕机，需要重新调度之前分配在上面的任务，并且重新生成worker slot资源
		// 	认为该机器上的所有任务全部失败，重新requeue，重新建立workerStatus
		impl.handleWorkerRestart(worker, req)
		worker = impl.workerStatus[req.WorkerUniqueID]
	}

	// seq较小的请求拒绝，防止状态回退
	if worker.HasSeq && req.Seq <= worker.LastSeq {
		codeOut = entity.HeartbeatOutputCodeSeqBackoffRequest
		return
	}

	// 可能上报新槽，这里扩展worker的slot槽位
	impl.workerExpandSlotsByReportedSlots(worker, req.SlotStatus)

	// 先处理 reports，再处理 slot snapshot。否则 worker 在同一轮心跳里上报
	// “任务已完成 + 槽已空闲”时，master 会先把旧 running task 误判为 lost。
	for _, report := range req.Reports {
		// TODO 暂时全部为true，这里也许以后有用
		if impl.handleTaskReportIdempotently(req.WorkerUniqueID, report) {
			respOut.AckedReports = append(respOut.AckedReports, report.Key())
		}
	}

	for _, jobID := range req.CleanupWantedJobs {
		if impl.canCleanupJobIntermediate(jobID) {
			respOut.CleanableJobs = append(respOut.CleanableJobs, jobID)
		}
	}

	worker.LastSeq = req.Seq
	worker.HasSeq = true
	worker.LastSeen = time.Now()
	worker.State = workerStateAlive
	respOut.ShouldStopAttempts = impl.updateWorkerSlotSnapshot(worker, req.SlotStatus)
	respOut.AssignedTasks = impl.assignTasksToWorker(worker)
	codeOut = entity.HeartbeatOutputCodeOK
}
