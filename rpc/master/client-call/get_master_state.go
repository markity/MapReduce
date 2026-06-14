package clientcall

import "mapreduce/rpc/comm"

// url: Get /client-api/master-state
type GetMasterStateResp struct {
	comm.RespComm

	Workers []MasterStateWorkerInfo `json:"workers"`
	Plugins []MasterStatePluginInfo `json:"plugins"`
	Jobs    []MasterStateJobInfo    `json:"jobs"`
}

type MasterStateWorkerInfo struct {
	WorkerUniqueID string                `json:"worker_unique_id"`
	WorkerAddr     string                `json:"worker_addr"`
	WorkerEpoch    int64                 `json:"worker_epoch"`
	WorkerState    string                `json:"worker_state"`
	LastSeen       string                `json:"last_seen"`
	LastSeq        uint64                `json:"last_seq"`
	HasSeq         bool                  `json:"has_seq"`
	Slots          []MasterStateSlotInfo `json:"slots"`
	FreeSlots      []string              `json:"free_slots"`
	BusySlots      []string              `json:"busy_slots"`
}

type MasterStateSlotInfo struct {
	SlotID             string                 `json:"slot_id"`
	CurrentRunningTask *MasterStateTaskAssign `json:"current_running_task,omitempty"`
}

type MasterStateTaskAssign struct {
	comm.TaskAttemptKey
	TaskType comm.TaskType `json:"task_type"`
}

type MasterStatePluginInfo struct {
	PluginUniqueID string `json:"plugin_unique_id"`
	ModTime        string `json:"mod_time"`
	PinCount       int    `json:"pin_count"`
}

type MasterStateJobInfo struct {
	JobID     string `json:"job_id"`
	JobName   string `json:"job_name"`
	Status    string `json:"status"`
	StartedAt string `json:"started_at"`
}
