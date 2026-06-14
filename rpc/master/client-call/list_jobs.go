package clientcall

import "mapreduce/rpc/comm"

type ListJobsRespJobEntry struct {
	JobID   string `json:"job_id"`
	JobName string `json:"job_name"`
	Status  string `json:"status"`
}

// url: Get /client/jobs
type ListJobsResp struct {
	comm.RespComm

	Jobs []ListJobsRespJobEntry `json:"jobs"`
}
