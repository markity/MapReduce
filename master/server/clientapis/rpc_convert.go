package clientapis

import (
	"fmt"
	"log"
	"mapreduce/master/entity"
	rpccomm "mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"time"
)

func taskAttemptKeyFromEntity(k entity.TaskAttemptKey) rpccomm.TaskAttemptKey {
	return rpccomm.TaskAttemptKey{
		JobID:     k.JobID,
		TaskID:    k.TaskID,
		AttemptID: k.AttemptID,
	}
}

func taskTypeFromEntity(taskType entity.TaskType) rpccomm.TaskType {
	switch taskType {
	case entity.TaskTypeMap:
		return rpccomm.TaskTypeMap
	case entity.TaskTypeReduce:
		return rpccomm.TaskTypeReduce
	}

	log.Println("warn: taskTypeFromEntity: task type invalid: " + fmt.Sprint(taskType))
	return ""
}

func listJobRespJobInfoEntryFromEntity(job entity.JobInfo) clientcall.ListJobsRespJobEntry {
	return clientcall.ListJobsRespJobEntry{
		JobID:   job.JobID,
		JobName: job.JobName,
		Status:  job.Status,
	}
}

func getJobRespJobInfoEntryFromEntity(job entity.JobInfo) clientcall.GetJobRespJobInfoEntry {
	return clientcall.GetJobRespJobInfoEntry{
		JobID:   job.JobID,
		JobName: job.JobName,
		Status:  job.Status,
	}
}

func createJobReqToEntity(req *clientcall.CreateMapReduceJobReq) *entity.CreateMapReduceJobInput {
	taskSplits := make([]entity.SplitSpec, 0, len(req.TaskSplits))
	for _, split := range req.TaskSplits {
		taskSplits = append(taskSplits, entity.SplitSpec{
			SplitType: string(split.SplitType),
			Data:      split.Data,
		})
	}
	return &entity.CreateMapReduceJobInput{
		JobName:        req.JobName,
		PluginUniqueID: req.PluginUniqueID,
		NumReduceTasks: req.NumReduceTasks,
		Conf:           cloneStringMap(req.Conf),
		TaskSplits:     taskSplits,
	}
}

func createJobRespFromEntity(resp *entity.CreateMapReduceJobOutput) *clientcall.CreateMapReduceJobResp {
	code := createJobCodeToRPC(resp.Code)
	return &clientcall.CreateMapReduceJobResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  rpccomm.GetMsgFromCode(code),
		},
		JobID: resp.JobID,
	}
}

func createJobCodeToRPC(code entity.CreateMapReduceJobCode) rpccomm.Code {
	switch code {
	case entity.CreateMapReduceJobCodeOK:
		return rpccomm.CodeOK
	case entity.CreateMapReduceJobCodePluginNotFound:
		return rpccomm.CodeCreateJobPluginNotFound
	case entity.CreateMapReduceJobCodeInternalError:
		return rpccomm.CodeInternalError
	default:
		return rpccomm.CodeInternalError
	}
}

func listJobsRespFromEntity(resp *entity.ListJobsOutput) *clientcall.ListJobsResp {
	code := listJobsCodeToRPC(resp.Code)
	jobs := make([]clientcall.ListJobsRespJobEntry, 0, len(resp.Jobs))
	for _, job := range resp.Jobs {
		jobs = append(jobs, listJobRespJobInfoEntryFromEntity(job))
	}
	return &clientcall.ListJobsResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  rpccomm.GetMsgFromCode(code),
		},
		Jobs: jobs,
	}
}

func listJobsCodeToRPC(code entity.ListJobsCode) rpccomm.Code {
	switch code {
	case entity.ListJobsCodeOK:
		return rpccomm.CodeOK
	case entity.ListJobsCodeInternalError:
		return rpccomm.CodeInternalError
	default:
		return rpccomm.CodeInternalError
	}
}

func getJobRespFromEntity(resp *entity.GetJobOutput) *clientcall.GetJobResp {
	code := getJobCodeToRPC(resp.Code)
	out := &clientcall.GetJobResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  rpccomm.GetMsgFromCode(code),
		},
	}
	if resp.Job != nil {
		job := getJobRespJobInfoEntryFromEntity(*resp.Job)
		out.Job = &job
	}
	return out
}

func getJobCodeToRPC(code entity.GetJobCode) rpccomm.Code {
	switch code {
	case entity.GetJobCodeOK:
		return rpccomm.CodeOK
	case entity.GetJobCodeNotFound:
		return rpccomm.CodeGetJobJobNotFound
	case entity.GetJobCodeInternalError:
		return rpccomm.CodeInternalError
	default:
		return rpccomm.CodeInternalError
	}
}

func getMasterStateRespFromEntity(resp *entity.GetMasterStateOutput) *clientcall.GetMasterStateResp {
	code := getMasterStateCodeToRPC(resp.Code)
	return &clientcall.GetMasterStateResp{
		RespComm: rpccomm.RespComm{
			Code: code,
			Msg:  rpccomm.GetMsgFromCode(code),
		},
		Workers: masterStateWorkersFromEntity(resp.Workers),
		Plugins: masterStatePluginsFromEntity(resp.Plugins),
		Jobs:    masterStateJobsFromEntity(resp.Jobs),
	}
}

func getMasterStateCodeToRPC(code entity.GetMasterStateCode) rpccomm.Code {
	switch code {
	case entity.GetMasterStateCodeOK:
		return rpccomm.CodeOK
	case entity.GetMasterStateCodeInternalError:
		return rpccomm.CodeInternalError
	default:
		return rpccomm.CodeInternalError
	}
}

func masterStateWorkersFromEntity(workers []entity.MasterStateWorkerInfo) []clientcall.MasterStateWorkerInfo {
	out := make([]clientcall.MasterStateWorkerInfo, 0, len(workers))
	for _, worker := range workers {
		out = append(out, masterStateWorkerFromEntity(worker))
	}
	return out
}

func masterStateWorkerFromEntity(worker entity.MasterStateWorkerInfo) clientcall.MasterStateWorkerInfo {
	return clientcall.MasterStateWorkerInfo{
		WorkerUniqueID: worker.WorkerUniqueID,
		WorkerAddr:     worker.WorkerAddr,
		WorkerEpoch:    worker.WorkerEpoch,
		WorkerState:    worker.WorkerState,
		LastSeen:       formatStateTime(worker.LastSeen),
		LastSeq:        worker.LastSeq,
		HasSeq:         worker.HasSeq,
		Slots:          masterStateSlotsFromEntity(worker.Slots),
		FreeSlots:      cloneStringSlice(worker.FreeSlots),
		BusySlots:      cloneStringSlice(worker.BusySlots),
	}
}

func masterStateSlotsFromEntity(slots []entity.MasterStateWorkerSlotInfo) []clientcall.MasterStateSlotInfo {
	out := make([]clientcall.MasterStateSlotInfo, 0, len(slots))
	for _, slot := range slots {
		out = append(out, clientcall.MasterStateSlotInfo{
			SlotID:             slot.SlotID,
			CurrentRunningTask: masterStateTaskAssignFromEntity(slot.CurrentRunningTask),
		})
	}
	return out
}

func masterStateTaskAssignFromEntity(task *entity.TaskSlotAssigned) *clientcall.MasterStateTaskAssign {
	if task == nil {
		return nil
	}
	return &clientcall.MasterStateTaskAssign{
		TaskAttemptKey: taskAttemptKeyFromEntity(task.TaskAttemptKey),
		TaskType:       taskTypeFromEntity(task.TaskType),
	}
}

func masterStatePluginsFromEntity(plugins []entity.MasterStatePluginInfo) []clientcall.MasterStatePluginInfo {
	out := make([]clientcall.MasterStatePluginInfo, 0, len(plugins))
	for _, plugin := range plugins {
		out = append(out, clientcall.MasterStatePluginInfo{
			PluginUniqueID: plugin.PluginUniqueID,
			ModTime:        formatStateTime(plugin.ModTime),
			PinCount:       plugin.PinCount,
		})
	}
	return out
}

func masterStateJobsFromEntity(jobs []entity.MasterStateJobInfo) []clientcall.MasterStateJobInfo {
	out := make([]clientcall.MasterStateJobInfo, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, clientcall.MasterStateJobInfo{
			JobID:     job.JobID,
			JobName:   job.JobName,
			Status:    job.Status,
			StartedAt: formatStateTime(job.StartedAt),
		})
	}
	return out
}

func formatStateTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneStringSlice(values []string) []string {
	out := make([]string, len(values))
	copy(out, values)
	return out
}
