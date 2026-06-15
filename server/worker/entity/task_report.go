package entity

import (
	"fmt"
	"log"
	"mapreduce/rpc/comm"
)

type TaskReportStatus string

const (
	TaskReportSucceeded TaskReportStatus = "succeeded"
	TaskReportFailed    TaskReportStatus = "failed"
)

type TaskReport struct {
	TaskAttemptKey
	WorkerUniqueID string
	WorkerAddr     string
	TaskType       TaskType

	// succeed or failed
	Status TaskReportStatus
	Error  string

	// Map任务成功时输出的map attempt位置
	MapOutput *MapOutputMetaEntry
	// Reduce任务，Error!=nil，失败时，可以输出哪些partition拉取失败
	//	master可以重新进行调度map任务
	LostMapOutputs []MapOutputMetaEntry
}

func (r TaskReport) Key() TaskAttemptKey {
	return r.TaskAttemptKey
}

func RpcCommTaskReportStatusFromEntity(status TaskReportStatus) comm.TaskReportStatus {
	switch status {
	case TaskReportSucceeded:
		return comm.TaskReportSucceeded
	case TaskReportFailed:
		return comm.TaskReportFailed
	}

	log.Println("warn: RpcCommTaskReportStatusFromEntity: task report status invalid: " + fmt.Sprint(status))
	return ""
}

func RpcCommMapOutputMetaEntryFromEntity(output MapOutputMetaEntry) comm.MapOutputMetaEntry {
	return comm.MapOutputMetaEntry{
		TaskAttemptKey: RpcCommTaskAttemptKeyFromEntity(output.TaskAttemptKey),
		WorkerUniqueID: output.WorkerUniqueID,
		WorkerAddr:     output.WorkerAddr,
		Size:           output.Size,
	}
}

func RpcCommMapOutputMetaEntryPtrFromEntity(output *MapOutputMetaEntry) *comm.MapOutputMetaEntry {
	if output == nil {
		return nil
	}
	commOutput := RpcCommMapOutputMetaEntryFromEntity(*output)
	return &commOutput
}

func RpcCommMapOutputMetaEntriesFromEntity(outputs []MapOutputMetaEntry) []comm.MapOutputMetaEntry {
	commOutputs := make([]comm.MapOutputMetaEntry, 0, len(outputs))
	for _, output := range outputs {
		commOutputs = append(commOutputs, RpcCommMapOutputMetaEntryFromEntity(output))
	}
	return commOutputs
}

func RpcCommTaskReportFromEntity(tr TaskReport) comm.TaskReport {
	return comm.TaskReport{
		TaskAttemptKey: RpcCommTaskAttemptKeyFromEntity(tr.TaskAttemptKey),
		TaskType:       RpcCommTaskTypeFromTaskType(tr.TaskType),
		Status:         RpcCommTaskReportStatusFromEntity(tr.Status),
		Error:          tr.Error,
		MapOutput:      RpcCommMapOutputMetaEntryPtrFromEntity(tr.MapOutput),
		LostMapOutputs: RpcCommMapOutputMetaEntriesFromEntity(tr.LostMapOutputs),
	}
}
