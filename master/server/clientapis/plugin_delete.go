package clientapis

import (
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"

	"github.com/gin-gonic/gin"
)

func DeletePlugin(pluginStorePath string) gin.HandlerFunc {
	return func(c *gin.Context) {
		pluginName := c.Param("plugin_unique_id")
		if _, ok := pluginPath(pluginStorePath, pluginName); !ok {
			c.JSON(http.StatusBadRequest, comm.RespComm{
				Code: comm.CodeBadRequest,
				Msg:  comm.GetMsgFromCode(comm.CodeBadRequest),
			})
			return
		}

		schedulerInstance := scheduler.GetScheduler()
		ok, err := schedulerInstance.DeletePlugin(pluginName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, comm.RespComm{
				Code: comm.CodeInternalError,
				Msg:  comm.GetMsgFromCode(comm.CodeInternalError),
			})
			return
		}
		if !ok {
			c.JSON(http.StatusNotFound, comm.RespComm{
				Code: comm.CodeDeletePluginPluginNotFound,
				Msg:  comm.GetMsgFromCode(comm.CodeDeletePluginPluginNotFound),
			})
			return
		}

		c.JSON(http.StatusOK, clientcall.DeletePluginResp{
			RespComm: comm.RespComm{
				Code: comm.CodeOK,
				Msg:  comm.GetMsgFromCode(comm.CodeOK),
			},
		})
	}
}
