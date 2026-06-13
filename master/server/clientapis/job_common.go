package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
)

func jobInfoFromEntity(job entity.JobInfo) clientcall.JobInfo {
	return clientcall.JobInfo{
		JobID:   job.JobID,
		JobName: job.JobName,
		Status:  job.Status,
	}
}

func badRequestResp() comm.RespComm {
	return comm.RespComm{
		Code: comm.CodeBadRequest,
		Msg:  comm.GetMsgFromCode(comm.CodeBadRequest),
	}
}
