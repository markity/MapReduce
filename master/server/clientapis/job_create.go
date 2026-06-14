package clientapis

import (
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
	return req.PluginUniqueID != "" && req.NumReduceTasks > 0 && len(req.TaskSplits) > 0
}
