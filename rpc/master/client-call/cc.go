package clientcall

import "mapreduce/rpc/comm"

// url: Post /client/upload-plugin/{插件名字}，body为插件binary blob
type UploadPluginResp struct {
	comm.RespComm

	PluginUniqueID string `json:"plugin_unique_id"`
}

// url: Delete /client/plugin/{pluginUniqueID}
type DeletePluginResp struct {
	comm.RespComm
}

// url: Get /client/plugins/{dict-order/time-order}
type ListPluginsResp struct {
	comm.RespComm

	Plugins     []string     `json:"plugins"` // plugin unique id list
	PluginInfos []PluginInfo `json:"plugin_infos"`
}

type PluginInfo struct {
	PluginUniqueID string `json:"plugin_unique_id"`
	UploadedAt     string `json:"uploaded_at"`
}

// url: Post /client/job，body为CreateMapReduceJobReq
type CreateMapReduceJobReq struct {
	JobName        string            `json:"job_name"`
	PluginUniqueID string            `json:"plugin_unique_id"`
	NumReduceTasks int               `json:"num_reduce_tasks"`
	Conf           map[string]string `json:"conf"`
}

// url: Post /client/job，response为CreateMapReduceJobResp
type CreateMapReduceJobResp struct {
	comm.RespComm

	JobID string `json:"job_id"`
}

// url: Get /client/jobs
type ListJobsResp struct {
	comm.RespComm

	Jobs []JobInfo `json:"jobs"`
}

// url: Get /client/jobs/{jobID}
type GetJobReq struct {
	JobID string `json:"job_id"`
}

// url: Get /client/jobs/{jobID}
type GetJobResp struct {
	comm.RespComm

	Job *JobInfo `json:"job"`
}

type JobInfo struct {
	JobID   string `json:"job_id"`
	JobName string `json:"job_name"`
	Status  string `json:"status"`
}

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
	JobID     string        `json:"job_id"`
	TaskID    string        `json:"task_id"`
	AttemptID string        `json:"attempt_id"`
	TaskType  comm.TaskType `json:"task_type"`
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
