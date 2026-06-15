package entity

import "time"

type GetMasterStateCode int

const (
	GetMasterStateCodeOK GetMasterStateCode = iota
)

const (
	GetMasterStateCodeInternalError GetMasterStateCode = CodeInternalError
)

type MasterStateWorkerSlotInfo struct {
	SlotID             string
	CurrentRunningTask *TaskSlotAssigned
}

type MasterStateWorkerInfo struct {
	WorkerUniqueID string
	WorkerAddr     string
	WorkerEpoch    int64
	WorkerState    string
	LastSeen       time.Time
	LastSeq        uint64
	HasSeq         bool

	Slots     []MasterStateWorkerSlotInfo
	FreeSlots []string
	BusySlots []string
}

type MasterStatePluginInfo struct {
	PluginUniqueID string
	ModTime        time.Time
	PinCount       int
}

type MasterStateJobInfo struct {
	JobID     string
	JobName   string
	Status    string
	StartedAt time.Time
}

type GetMasterStateOutput struct {
	Code GetMasterStateCode

	Workers []MasterStateWorkerInfo
	Plugins []MasterStatePluginInfo
	Jobs    []MasterStateJobInfo
}
