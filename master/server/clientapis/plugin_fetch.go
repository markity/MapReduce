package clientapis

import (
	"fmt"
	"log"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func FetchPlugin() gin.HandlerFunc {
	return func(c *gin.Context) {
		pluginUniqueID := c.Param("plugin_unique_id")
		if pluginUniqueID == "" {
			c.JSON(http.StatusBadRequest, fetchPluginFailureResp(comm.CodeBadRequest))
			return
		}

		fetched := false
		var pin string
		output := scheduler.GetScheduler().GetPluginFilePathAndPin(pluginUniqueID, &pin)
		defer func() {
			if fetched && scheduler.GetScheduler().PluginFileUnpin(pluginUniqueID, pin) != true {
				log.Println("warn: failed to unpin plugin, check code")
			}
		}()

		switch output.Code {
		case scheduler.GetPluginFilePathCodeOK:
			fetched = true
		case scheduler.GetPluginFilePathCodePluginNotFound:
			c.JSON(http.StatusNotFound, fetchPluginFailureResp(comm.CodeFetchPluginPluginNotFound))
			return
		default:
			c.JSON(http.StatusInternalServerError, fetchPluginFailureResp(comm.CodeInternalError))
			return
		}

		s, err := os.Stat(output.FilePath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, fetchPluginFailureResp(comm.CodeInternalError))
			return
		}

		c.Header("Content-Length", fmt.Sprint(s.Size()))
		c.Header("Content-Type", "application/octet-stream")
		c.File(output.FilePath)
	}
}

func fetchPluginFailureResp(code comm.Code) clientcall.FetchPluginByPluginIDRespOnFailure {
	return clientcall.FetchPluginByPluginIDRespOnFailure{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
	}
}
