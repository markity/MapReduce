package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		resp := scheduler.GetScheduler().GetJob(&entity.GetJobInput{
			JobID: c.Param("id"),
		})
		dto := getJobRespFromEntity(resp)
		if dto.Code != comm.CodeOK {
			c.JSON(http.StatusNotFound, dto)
			return
		}
		c.JSON(http.StatusOK, dto)
	}
}

func getJobRespFromEntity(resp *entity.GetJobOutput) *clientcall.GetJobResp {
	code := getJobCodeToRPC(resp.Code)
	out := &clientcall.GetJobResp{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
	}
	if resp.Job != nil {
		job := jobInfoFromEntity(*resp.Job)
		out.Job = &job
	}
	return out
}

func getJobCodeToRPC(code entity.GetJobCode) comm.Code {
	switch code {
	case entity.GetJobCodeOK:
		return comm.CodeOK
	case entity.GetJobCodeNotFound:
		return comm.CodeGetJobJobNotFound
	case entity.GetJobCodeInternalError:
		return comm.CodeInternalError
	default:
		return comm.CodeInternalError
	}
}
