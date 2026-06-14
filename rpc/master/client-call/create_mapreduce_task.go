package clientcall

import "mapreduce/rpc/comm"

// url: Post /client/job，body为CreateMapReduceJobReq
type CreateMapReduceJobReq struct {
	JobName        string            `json:"job_name"`
	PluginUniqueID string            `json:"plugin_unique_id"`
	NumReduceTasks int               `json:"num_reduce_tasks"`
	Conf           map[string]string `json:"conf"`
	TaskSplits     []comm.SplitSpec  `json:"task_splits"`
}

// url: Post /client/job，response为CreateMapReduceJobResp
type CreateMapReduceJobResp struct {
	comm.RespComm

	JobID string `json:"job_id"`
}
