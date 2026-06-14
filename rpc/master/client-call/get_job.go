package clientcall

import "mapreduce/rpc/comm"

// url: Get /client/jobs/{jobID}
type GetJobReq struct {
	JobID string `json:"job_id"`
}

// url: Get /client/jobs/{jobID}
type GetJobResp struct {
	comm.RespComm

	Job *GetJobRespJobInfoEntry `json:"job"`
}

type GetJobRespJobInfoEntry struct {
	JobID   string `json:"job_id"`
	JobName string `json:"job_name"`
	Status  string `json:"status"`
}
