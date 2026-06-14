package clientapis

import (
	"mapreduce/master/entity"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
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
