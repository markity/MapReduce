package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"

	"github.com/gin-gonic/gin"
)

func CreateMapReduceJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req clientcall.CreateMapReduceJobReq
		if err := c.BindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, badRequestResp())
			return
		}
		if !validCreateMapReduceJobReq(&req) {
			c.JSON(http.StatusBadRequest, badRequestResp())
			return
		}

		resp := scheduler.GetScheduler().CreateMapReduceJob(createJobReqToEntity(&req))
		dto := createJobRespFromEntity(resp)
		if dto.Code == comm.CodeCreateJobPluginNotFound {
			c.JSON(http.StatusNotFound, dto)
			return
		}
		if dto.Code != comm.CodeOK {
			c.JSON(http.StatusInternalServerError, dto)
			return
		}
		c.JSON(http.StatusOK, dto)
	}
}

func validCreateMapReduceJobReq(req *clientcall.CreateMapReduceJobReq) bool {
	return req.PluginUniqueID != "" && len(req.TaskSplits) > 0 && req.NumReduceTasks > 0
}

func createJobReqToEntity(req *clientcall.CreateMapReduceJobReq) *entity.CreateMapReduceJobInput {
	taskSplits := make([]entity.SplitSpec, 0, len(req.TaskSplits))
	for _, split := range req.TaskSplits {
		taskSplits = append(taskSplits, entity.SplitSpec{
			SplitType: string(split.SplitType),
			Data:      split.Data,
		})
	}
	return &entity.CreateMapReduceJobInput{
		JobName:        req.JobName,
		PluginUniqueID: req.PluginUniqueID,
		NumReduceTasks: req.NumReduceTasks,
		TaskSplits:     taskSplits,
	}
}

func createJobRespFromEntity(resp *entity.CreateMapReduceJobOutput) *clientcall.CreateMapReduceJobResp {
	code := createJobCodeToRPC(resp.Code)
	return &clientcall.CreateMapReduceJobResp{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
		JobID: resp.JobID,
	}
}

func createJobCodeToRPC(code entity.CreateMapReduceJobCode) comm.Code {
	switch code {
	case entity.CreateMapReduceJobCodeOK:
		return comm.CodeOK
	case entity.CreateMapReduceJobCodePluginNotFound:
		return comm.CodeCreateJobPluginNotFound
	case entity.CreateMapReduceJobCodeInternalError:
		return comm.CodeInternalError
	default:
		return comm.CodeInternalError
	}
}
