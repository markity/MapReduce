package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"

	"github.com/gin-gonic/gin"
)

func ListJobs() gin.HandlerFunc {
	return func(c *gin.Context) {
		resp := scheduler.GetScheduler().ListJobs()
		c.JSON(http.StatusOK, listJobsRespFromEntity(resp))
	}
}

func listJobsRespFromEntity(resp *entity.ListJobsOutput) *clientcall.ListJobsResp {
	code := listJobsCodeToRPC(resp.Code)
	jobs := make([]clientcall.JobInfo, 0, len(resp.Jobs))
	for _, job := range resp.Jobs {
		jobs = append(jobs, jobInfoFromEntity(job))
	}
	return &clientcall.ListJobsResp{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
		Jobs: jobs,
	}
}

func listJobsCodeToRPC(code entity.ListJobsCode) comm.Code {
	switch code {
	case entity.ListJobsCodeOK:
		return comm.CodeOK
	case entity.ListJobsCodeInternalError:
		return comm.CodeInternalError
	default:
		return comm.CodeInternalError
	}
}
