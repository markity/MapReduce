package clientapis

import (
	"mapreduce/server/master/scheduler"
	"net/http"

	"github.com/gin-gonic/gin"
)

func ListJobs() gin.HandlerFunc {
	return func(c *gin.Context) {
		resp := scheduler.GetScheduler().ListJobs()
		c.JSON(http.StatusOK, listJobsRespFromEntity(resp))
	}
}
