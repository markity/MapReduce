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
	JobID     string `json:"job_id"`
	TaskID    string `json:"task_id"`
	AttemptID string `json:"attempt_id"`
}

type TaskSlotAssigned struct {
	SlotID string `json:"slot_id"`
	TaskAttemptKey
	TaskType   TaskType          `json:"task_type"`
	Plugin     PluginSpec        `json:"plugin_spec"`
	Conf       map[string]string `json:"conf"`
	MapTask    *MapTaskSpec      `json:"map_task"`
	ReduceTask *ReduceTaskSpec   `json:"reduce_task"`
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
