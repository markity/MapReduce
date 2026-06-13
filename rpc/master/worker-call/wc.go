package workercall

import (
	"mapreduce/rpc/comm"
)

type HeartbeatReq struct {
	WorkerUniqueID string `json:"worker_unique_id"`
	// worker可能宕机重启，调度器可以通过这个随着时间递增的epoch感知到
	WorkerEpoch int64  `json:"worker_epoch"`
	WorkerAddr  string `json:"worker_addr"`

	// seq可以解决心跳后发先至，seq较小的请求将被拒绝
	Seq uint64 `json:"seq"`

	// 当前状态快照
	SlotStatus map[string]comm.TaskSlotStatus `json:"slots"`

	// 自从本地pending队列取完成/失败的任务，每次心跳都上报，后续master会对(jid, tid, aid)维度进行确认，删除pending列表里的待确认任务
	Reports []comm.TaskReport `json:"reports"`

	// 关注某个job是否执行完毕，后续进行磁盘清理
	CleanupWantedJobs []string `json:"cleanup_wanted_jobs"`
}

type AssignTask struct {
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

type HeartbeatResp struct {
	comm.RespComm

	AckedReports []comm.TaskAttemptKey `json:"acked_reports"`

	// 将CleanupWantedJobs内已完成的job_id返回，它一定是CleanupWantedJobs的子集
	CleanableJobs []string `json:"cleanable_jobs"`

	AssignedTasks []AssignTask `json:"assigned_tasks"`

	ShouldStopAttempts []comm.TaskAttemptKey `json:"should_stop_attempts"`
}

// 200ok response可能是二进制，失败的时候非200,返回json
type FetchPluginRespOnFailure struct {
	comm.RespComm
}
