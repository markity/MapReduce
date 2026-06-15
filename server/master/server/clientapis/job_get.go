package clientapis

import (
	"mapreduce/rpc/comm"
	"mapreduce/server/master/entity"
	"mapreduce/server/master/scheduler"
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
