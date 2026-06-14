package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func GetMasterState() gin.HandlerFunc {
	return func(c *gin.Context) {
		resp := scheduler.GetScheduler().GetMasterState()
		c.JSON(http.StatusOK, getMasterStateRespFromEntity(resp))
	}
}

func getMasterStateRespFromEntity(resp *entity.GetMasterStateOutput) *clientcall.GetMasterStateResp {
	code := getMasterStateCodeToRPC(resp.Code)
	return &clientcall.GetMasterStateResp{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
		Workers: masterStateWorkersFromEntity(resp.Workers),
		Plugins: masterStatePluginsFromEntity(resp.Plugins),
		Jobs:    masterStateJobsFromEntity(resp.Jobs),
	}
}

func getMasterStateCodeToRPC(code entity.GetMasterStateCode) comm.Code {
	switch code {
	case entity.GetMasterStateCodeOK:
		return comm.CodeOK
	case entity.GetMasterStateCodeInternalError:
		return comm.CodeInternalError
	default:
		return comm.CodeInternalError
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
		JobID:     task.JobID,
		TaskID:    task.TaskID,
		AttemptID: task.AttemptID,
		TaskType:  comm.TaskType(task.TaskType),
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

func cloneStringSlice(values []string) []string {
	out := make([]string, len(values))
	copy(out, values)
	return out
}
