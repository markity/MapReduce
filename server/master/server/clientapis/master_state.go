package clientapis

import (
	"mapreduce/server/master/scheduler"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetMasterState() gin.HandlerFunc {
	return func(c *gin.Context) {
		resp := scheduler.GetScheduler().GetMasterState()
		c.JSON(http.StatusOK, getMasterStateRespFromEntity(resp))
	}
}
