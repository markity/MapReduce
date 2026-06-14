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

type PluginSpecType string

const (
	FromLocalFS PluginSpecType = "from-local-fs"
	FromMaster  PluginSpecType = "from-master"
)

type PluginSpec struct {
	Type   PluginSpecType
	URI    string
	SHA256 string
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
