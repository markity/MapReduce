package mastercall

import "mapreduce/rpc/comm"

type RunTaskReq struct {
	SlotID    string        `json:"slot_id"`
	JobID     string        `json:"job_id"`
	TaskID    string        `json:"task_id"`
	AttemptID string        `json:"attempt_id"`
	TaskType  comm.TaskType `json:"task_type"`

	Plugin comm.PluginSpec   `json:"plugin_spec"`
	Conf   map[string]string `json:"conf"`

	// 二选一
	MapTask    *comm.MapTaskSpec    `json:"map_task,omitempty"`
	ReduceTask *comm.ReduceTaskSpec `json:"reduce_task,omitempty"`
}

type RunTaskResp struct {
	comm.RespComm
	SlotID *string `json:"slot_id"`
}

type FetchMapOutputReq struct {
	JobID     string `json:"job_id"`
	MapTaskID string `json:"map_task_id"`
	AttemptID string `json:"attempt_id"`

	PartitionID int `json:"partition_id"`
}

// FetchMapOutputResp是file作为output，不是json形式
