package workerapis

import (
	"fmt"
	"log"
	"mapreduce/master/entity"
	rpccomm "mapreduce/rpc/comm"
)

func taskAttemptKeyToEntity(k rpccomm.TaskAttemptKey) entity.TaskAttemptKey {
	return entity.TaskAttemptKey{
		JobID:     k.JobID,
		TaskID:    k.TaskID,
		AttemptID: k.AttemptID,
	}
}

func taskAttemptKeyFromEntity(k entity.TaskAttemptKey) rpccomm.TaskAttemptKey {
	return rpccomm.TaskAttemptKey{
		JobID:     k.JobID,
		TaskID:    k.TaskID,
		AttemptID: k.AttemptID,
	}
}

func taskAttemptKeysFromEntity(ks []entity.TaskAttemptKey) []rpccomm.TaskAttemptKey {
	out := make([]rpccomm.TaskAttemptKey, 0, len(ks))
	for _, k := range ks {
		out = append(out, taskAttemptKeyFromEntity(k))
	}
	return out
}

func taskTypeToEntity(taskType rpccomm.TaskType) entity.TaskType {
	switch taskType {
	case rpccomm.TaskTypeMap:
		return entity.TaskTypeMap
	case rpccomm.TaskTypeReduce:
		return entity.TaskTypeReduce
	}

	log.Println("warn: taskTypeToEntity, unknowne val" + fmt.Sprint(taskType))
	return entity.TaskType("")
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

func mapOutputMetaToEntity(output rpccomm.MapOutputMetaEntry) entity.MapOutputMetaEntry {
	return entity.MapOutputMetaEntry{
		TaskAttemptKey: taskAttemptKeyToEntity(output.TaskAttemptKey),
		WorkerUniqueID: output.WorkerUniqueID,
		WorkerAddr:     output.WorkerAddr,
		Size:           output.Size,
	}
}

func mapOutputMetaFromEntity(output entity.MapOutputMetaEntry) rpccomm.MapOutputMetaEntry {
	return rpccomm.MapOutputMetaEntry{
		TaskAttemptKey: taskAttemptKeyFromEntity(output.TaskAttemptKey),
		WorkerUniqueID: output.WorkerUniqueID,
		WorkerAddr:     output.WorkerAddr,
		Size:           output.Size,
	}
}

func mapOutputMetasFromEntity(outputs []entity.MapOutputMetaEntry) []rpccomm.MapOutputMetaEntry {
	rpcOutputs := make([]rpccomm.MapOutputMetaEntry, 0, len(outputs))
	for _, output := range outputs {
		rpcOutputs = append(rpcOutputs, mapOutputMetaFromEntity(output))
	}
	return rpcOutputs
}

func mapOutputMetasToEntity(outputs []rpccomm.MapOutputMetaEntry) []entity.MapOutputMetaEntry {
	entityOutputs := make([]entity.MapOutputMetaEntry, 0, len(outputs))
	for _, output := range outputs {
		entityOutputs = append(entityOutputs, mapOutputMetaToEntity(output))
	}
	return entityOutputs
}

func mapOutputMetaPtrToEntity(output *rpccomm.MapOutputMetaEntry) *entity.MapOutputMetaEntry {
	if output == nil {
		return nil
	}
	entityOutput := mapOutputMetaToEntity(*output)
	return &entityOutput
}
