package entity

import (
	"fmt"
	"log"
	"mapreduce/rpc/comm"
)

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type TaskAttemptKey struct {
	JobID     string
	TaskID    string
	AttemptID string
}

type TaskSlotAssigned struct {
	SlotID string
	TaskAttemptKey
	TaskType   TaskType
	Plugin     PluginSpec
	Conf       map[string]string
	MapTask    *MapTaskSpec
	ReduceTask *ReduceTaskSpec
}

type TaskSlotStatus struct {
	SlotID             string
	CurrentRunningTask *TaskSlotAssigned
}

func RpcCommTaskAttemptKeyFromEntity(key TaskAttemptKey) comm.TaskAttemptKey {
	return comm.TaskAttemptKey{
		JobID:     key.JobID,
		TaskID:    key.TaskID,
		AttemptID: key.AttemptID,
	}
}

func TaskAttemptKeyFromRpcComm(key comm.TaskAttemptKey) TaskAttemptKey {
	return TaskAttemptKey{
		JobID:     key.JobID,
		TaskID:    key.TaskID,
		AttemptID: key.AttemptID,
	}
}

func TaskAttemptKeysFromRpcComm(keys []comm.TaskAttemptKey) []TaskAttemptKey {
	out := make([]TaskAttemptKey, 0, len(keys))
	for _, key := range keys {
		out = append(out, TaskAttemptKeyFromRpcComm(key))
	}
	return out
}

type RunningTask struct {
	TaskAttemptKey
	TaskType TaskType
}

func RunningTaskFromTaskSlotAssigned(assign *TaskSlotAssigned) *RunningTask {
	if assign == nil {
		return nil
	}
	return &RunningTask{
		TaskAttemptKey: assign.TaskAttemptKey,
		TaskType:       assign.TaskType,
	}
}

func RpcCommTaskTypeFromTaskType(tt TaskType) comm.TaskType {
	switch tt {
	case TaskTypeMap:
		return comm.TaskTypeMap
	case TaskTypeReduce:
		return comm.TaskTypeReduce
	}

	log.Println("warn: RpcCommTaskTypeFromTaskType: task type invalid: " + fmt.Sprint(tt))
	return ""
}

func RpcCommRunningTaskFromRunningTask(rt *RunningTask) *comm.TaskSlotStatusReportRunningTask {
	if rt == nil {
		return nil
	}
	return &comm.TaskSlotStatusReportRunningTask{
		TaskAttemptKey: RpcCommTaskAttemptKeyFromEntity(rt.TaskAttemptKey),
		TaskType:       RpcCommTaskTypeFromTaskType(rt.TaskType),
	}
}

// func RpcCommAssignedFromEntity(assign *TaskSlotAssigned) *comm.AssignTask {
// 	if assign == nil {
// 		return nil
// 	}

// 	return &comm.AssignTask{
// 		TaskAttemptKey: RpcCommTaskAttemptKeyFromEntity(assign.TaskAttemptKey),
// 		SlotID:         assign.SlotID,
// 		TaskType:       comm.TaskType(assign.TaskType),
// 		Plugin:         comm.PluginSpec{
// 			Type: assign.pl,
// 		},
// 	}
// }
