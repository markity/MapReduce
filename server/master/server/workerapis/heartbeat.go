package workerapis

import (
	"fmt"
	"mapreduce/rpc/comm"
	rpccomm "mapreduce/rpc/comm"
	workercall "mapreduce/rpc/master/worker-call"
	"mapreduce/server/master/entity"
	"mapreduce/server/master/scheduler"
	"net/http"

	"github.com/gin-gonic/gin"
)

func failHeartbeatResp(code rpccomm.Code, msg string) *workercall.HeartbeatResp {
	return &workercall.HeartbeatResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  msg,
		},
	}
}

func entityHeartbeatCodeToRPC(code entity.HeartbeatOutputCode) rpccomm.Code {
	switch code {
	case entity.HeartbeatOutputCodeOK:
		return rpccomm.CodeOK
	case entity.HeartbeatOutputCodeSeqBackoffRequest:
		return rpccomm.CodeHeartbeatSeqBackoffRequest
	case entity.HeartbeatOutputCodeEpochStaleRequest:
		return rpccomm.CodeHeartbeatEpochStaleRequest
	case entity.HeartbeatOutputCodeInternalError:
		return rpccomm.CodeInternalError
	default:
		return rpccomm.CodeInternalError
	}
}

func taskReportStatusToEntity(status rpccomm.TaskReportStatus) entity.TaskReportStatus {
	switch status {
	case rpccomm.TaskReportSucceeded:
		return entity.TaskReportSucceeded
	case rpccomm.TaskReportFailed:
		return entity.TaskReportFailed
	default:
		return entity.TaskReportStatus(status)
	}
}

func mapTaskFromEntity(task *entity.MapTaskSpec) *rpccomm.MapTaskSpec {
	if task == nil {
		return nil
	}
	return &rpccomm.MapTaskSpec{
		NumReduce: task.NumReduce,
		Split: rpccomm.SplitSpec{
			SplitType: rpccomm.SplitType(task.Split.SplitType),
			Data:      task.Split.Data,
		},
	}
}

func reduceTaskFromEntity(task *entity.ReduceTaskSpec) *rpccomm.ReduceTaskSpec {
	if task == nil {
		return nil
	}
	return &rpccomm.ReduceTaskSpec{
		ReducePartitionID: task.ReducePartitionID,
		MapOutputs:        mapOutputMetasFromEntity(task.MapOutputs),
	}
}

func assignedTaskFromEntity(task entity.AssignedTask) comm.AssignTask {
	return comm.AssignTask{
		TaskAttemptKey: taskAttemptKeyFromEntity(task.TaskAttemptKey),
		SlotID:         task.SlotID,
		TaskType:       taskTypeFromEntity(task.TaskType),
		Plugin:         pluginSpecFromEntity(task.Plugin),
		Conf:           cloneStringMap(task.Conf),
		MapTask:        mapTaskFromEntity(task.MapTask),
		ReduceTask:     reduceTaskFromEntity(task.ReduceTask),
	}
}

func pluginSpecFromEntity(plugin entity.PluginSpec) rpccomm.PluginSpec {
	return rpccomm.PluginSpec{
		Type:           rpccomm.PluginSpecType(plugin.Type),
		PluginUniqueID: plugin.PluginUniqueID,
		URI:            plugin.URI,
		SHA256:         plugin.SHA256,
	}
}

func assignedTasksFromEntity(tasks []entity.AssignedTask) []comm.AssignTask {
	rpcTasks := make([]comm.AssignTask, 0, len(tasks))
	for _, task := range tasks {
		rpcTasks = append(rpcTasks, assignedTaskFromEntity(task))
	}
	return rpcTasks
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func runningTaskToEntity(task *rpccomm.TaskSlotStatusReportRunningTask) *entity.TaskSlotAssigned {
	if task == nil {
		return nil
	}
	return &entity.TaskSlotAssigned{
		TaskAttemptKey: taskAttemptKeyToEntity(task.TaskAttemptKey),
		TaskType:       taskTypeToEntity(task.TaskType),
	}
}

func taskSlotsToEntity(slots map[string]rpccomm.TaskSlotStatusReport) map[string]entity.TaskSlotStatus {
	entitySlots := make(map[string]entity.TaskSlotStatus, len(slots))
	for key, slot := range slots {
		entitySlots[key] = entity.TaskSlotStatus{
			SlotID:             slot.SlotID,
			CurrentRunningTask: runningTaskToEntity(slot.CurrentRunningTask),
		}
	}
	return entitySlots
}

func taskReportsToEntity(reports []rpccomm.TaskReport, workerUniqueID string, workerAddr string) []entity.TaskReport {
	entityReports := make([]entity.TaskReport, 0, len(reports))
	for _, report := range reports {
		entityReports = append(entityReports, entity.TaskReport{
			TaskAttemptKey: taskAttemptKeyToEntity(report.TaskAttemptKey),
			TaskType:       taskTypeToEntity(report.TaskType),
			Status:         taskReportStatusToEntity(report.Status),
			Error:          report.Error,
			MapOutput:      mapOutputMetaPtrToEntity(report.MapOutput),
			LostMapOutputs: mapOutputMetasToEntity(report.LostMapOutputs),
			WorkerUniqueID: workerUniqueID,
			WorkerAddr:     workerAddr,
		})
	}
	return entityReports
}

func heartbeatReqToEntity(req *workercall.HeartbeatReq) *entity.HeartbeatInput {
	return &entity.HeartbeatInput{
		WorkerUniqueID:    req.WorkerUniqueID,
		WorkerEpoch:       req.WorkerEpoch,
		WorkerAddr:        req.WorkerAddr,
		Seq:               req.Seq,
		SlotStatus:        taskSlotsToEntity(req.SlotStatus),
		Reports:           taskReportsToEntity(req.Reports, req.WorkerUniqueID, req.WorkerAddr),
		CleanupWantedJobs: req.CleanupWantedJobs,
	}
}

func validHeartbeatReq(req *workercall.HeartbeatReq) bool {
	if req.WorkerUniqueID == "" || req.WorkerAddr == "" || req.WorkerEpoch < 0 {
		return false
	}
	for key, slot := range req.SlotStatus {
		if slot.SlotID == "" || key != slot.SlotID {
			return false
		}
		if slot.CurrentRunningTask != nil && !validRunningTaskStatus(slot.CurrentRunningTask) {
			return false
		}
	}
	for _, report := range req.Reports {
		if !validTaskReport(report) {
			return false
		}
	}
	for _, jobID := range req.CleanupWantedJobs {
		if jobID == "" {
			return false
		}
	}
	return true
}

func validRunningTaskStatus(task *rpccomm.TaskSlotStatusReportRunningTask) bool {
	return task.JobID != "" &&
		task.TaskID != "" &&
		task.AttemptID != "" &&
		validTaskType(task.TaskType)
}

func validTaskReport(report rpccomm.TaskReport) bool {
	return report.JobID != "" &&
		report.TaskID != "" &&
		report.AttemptID != "" &&
		validTaskType(report.TaskType) &&
		validTaskReportStatus(report.Status) &&
		validMapOutput(report.MapOutput) &&
		validMapOutputs(report.LostMapOutputs)
}

func validTaskType(taskType rpccomm.TaskType) bool {
	return taskType == rpccomm.TaskTypeMap || taskType == rpccomm.TaskTypeReduce
}

func validTaskReportStatus(status rpccomm.TaskReportStatus) bool {
	return status == rpccomm.TaskReportSucceeded || status == rpccomm.TaskReportFailed
}

func validMapOutputs(outputs []rpccomm.MapOutputMetaEntry) bool {
	for _, output := range outputs {
		if output.JobID == "" ||
			output.TaskID == "" ||
			output.AttemptID == "" ||
			output.WorkerUniqueID == "" ||
			output.WorkerAddr == "" ||
			output.Size < 0 {
			return false
		}
	}
	return true
}

func validMapOutput(output *rpccomm.MapOutputMetaEntry) bool {
	if output == nil {
		return true
	}
	return validMapOutputs([]rpccomm.MapOutputMetaEntry{*output})
}

func heartbeatRespFromEntity(resp *entity.HeartbeatOutput) *workercall.HeartbeatResp {
	code := entityHeartbeatCodeToRPC(resp.Code)
	rpcResp := &workercall.HeartbeatResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  rpccomm.GetMsgFromCode(code),
		},
		AckedReports:       make([]rpccomm.TaskAttemptKey, 0, len(resp.AckedReports)),
		CleanableJobs:      resp.CleanableJobs,
		AssignedTasks:      assignedTasksFromEntity(resp.AssignedTasks),
		ShouldStopAttempts: taskAttemptKeysFromEntity(resp.ShouldStopAttempts),
	}
	for _, key := range resp.AckedReports {
		rpcResp.AckedReports = append(rpcResp.AckedReports, taskAttemptKeyFromEntity(key))
	}
	return rpcResp
}

func Heartbeat() gin.HandlerFunc {
	return func(c *gin.Context) {
		fmt.Println("heartbeat")
		var heartbeatReq workercall.HeartbeatReq
		err := c.BindJSON(&heartbeatReq)
		if err != nil {
			c.JSON(http.StatusBadRequest, failHeartbeatResp(rpccomm.CodeBadRequest, rpccomm.GetMsgFromCode(rpccomm.CodeBadRequest)))
			return
		}
		if !validHeartbeatReq(&heartbeatReq) {
			c.JSON(http.StatusBadRequest, failHeartbeatResp(rpccomm.CodeBadRequest, rpccomm.GetMsgFromCode(rpccomm.CodeBadRequest)))
			return
		}

		resp := scheduler.GetScheduler().PostHeartbeatReq(heartbeatReqToEntity(&heartbeatReq))
		c.JSON(http.StatusOK, heartbeatRespFromEntity(resp))
	}
}
