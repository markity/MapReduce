package entity

import (
	"encoding/json"
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

	MapTask    *MapTaskSpec
	ReduceTask *ReduceTaskSpec
}
