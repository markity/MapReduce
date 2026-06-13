package clientcall

import "mapreduce/rpc/comm"

// url: Post /client/upload-plugin/{插件名字}，body为插件binary blob
type UploadPluginResp struct {
	comm.RespComm

	PluginUniqueID string `json:"plugin_unique_id"`
}

// url: Delete /client/plugin/{uniqueID}
type DeletePluginResp struct {
	comm.RespComm
}

// url: Get /client/plugins/{dict-order/time-order}
type ListPluginsResp struct {
	comm.RespComm

	Plugins []string `json:"plugins"` // 插件的 UniqueID 列表
}

// url: Post /client/job，body为CreateMapReduceJobReq
type CreateMapReduceJobReq struct {
	JobName        string           `json:"job_name"`
	PluginUniqueID string           `json:"plugin_unique_id"`
	NumReduceTasks int              `json:"num_reduce_tasks"`
	TaskSplits     []comm.SplitSpec `json:"task_splits"`
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
