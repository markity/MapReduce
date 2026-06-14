package entity

import (
	"encoding/json"
	"mapreduce/rpc/comm"
)

type MapTaskSpec struct {
	NumReduce int
	Split     SplitSpec
}

type SplitSpec struct {
	SplitType string
	Data      json.RawMessage
}

type MapOutputMetaEntry struct {
	WorkerUniqueID string
	WorkerAddr     string

	TaskAttemptKey

	Size int64
}

type ReduceTaskSpec struct {
	ReducePartitionID int
	JobID             string
	MapOutputs        []MapOutputMetaEntry
}

type AssignedTask struct {
	TaskAttemptKey
	SlotID   string
	TaskType TaskType

	Plugin PluginSpec
	Conf   map[string]string

	MapTask    *MapTaskSpec
	ReduceTask *ReduceTaskSpec
}

func AssignedTaskFromRpcComm(task comm.AssignTask) AssignedTask {
	return AssignedTask{
		TaskAttemptKey: TaskAttemptKeyFromRpcComm(task.TaskAttemptKey),
		SlotID:         task.SlotID,
		TaskType:       TaskType(task.TaskType),
		Plugin:         pluginSpecFromRpcComm(task.Plugin),
		Conf:           task.Conf,
		MapTask:        mapTaskSpecFromRpcComm(task.MapTask),
		ReduceTask:     reduceTaskSpecFromRpcComm(task.ReduceTask),
	}
}

func TaskSlotAssignedFromAssignedTask(task AssignedTask) TaskSlotAssigned {
	return TaskSlotAssigned{
		SlotID:         task.SlotID,
		TaskAttemptKey: task.TaskAttemptKey,
		TaskType:       task.TaskType,
		Plugin:         task.Plugin,
		Conf:           task.Conf,
		MapTask:        task.MapTask,
		ReduceTask:     task.ReduceTask,
	}
}

func pluginSpecFromRpcComm(plugin comm.PluginSpec) PluginSpec {
	return PluginSpec{
		Type:           PluginSpecType(plugin.Type),
		PluginUniqueID: plugin.PluginUniqueID,
		URI:            plugin.URI,
		SHA256:         plugin.SHA256,
	}
}

func mapTaskSpecFromRpcComm(task *comm.MapTaskSpec) *MapTaskSpec {
	if task == nil {
		return nil
	}
	return &MapTaskSpec{
		NumReduce: task.NumReduce,
		Split: SplitSpec{
			SplitType: string(task.Split.SplitType),
			Data:      task.Split.Data,
		},
	}
}

func reduceTaskSpecFromRpcComm(task *comm.ReduceTaskSpec) *ReduceTaskSpec {
	if task == nil {
		return nil
	}
	return &ReduceTaskSpec{
		ReducePartitionID: task.ReducePartitionID,
		MapOutputs:        mapOutputMetaEntriesFromRpcComm(task.MapOutputs),
	}
}

func mapOutputMetaEntriesFromRpcComm(outputs []comm.MapOutputMetaEntry) []MapOutputMetaEntry {
	result := make([]MapOutputMetaEntry, 0, len(outputs))
	for _, output := range outputs {
		result = append(result, MapOutputMetaEntry{
			WorkerUniqueID: output.WorkerUniqueID,
			WorkerAddr:     output.WorkerAddr,
			TaskAttemptKey: TaskAttemptKeyFromRpcComm(output.TaskAttemptKey),
			Size:           output.Size,
		})
	}
	return result
}
