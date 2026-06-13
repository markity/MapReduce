package clientapis

import (
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
)

func ListPlugins() gin.HandlerFunc {
	return func(c *gin.Context) {
		order := c.Param("order")
		plugins := scheduler.GetScheduler().ListPlugins()
		if !sortPluginSnapshots(plugins, order) {
			c.JSON(http.StatusBadRequest, comm.RespComm{
				Code: comm.CodeBadRequest,
				Msg:  comm.GetMsgFromCode(comm.CodeBadRequest),
			})
			return
		}
		c.JSON(http.StatusOK, okListPluginsResp(pluginUniqueIDs(plugins)))
	}
}

func sortPluginSnapshots(plugins []scheduler.PluginSnapshot, order string) bool {
	switch order {
	case "dict-order":
		sort.Slice(plugins, func(i, j int) bool {
			return plugins[i].PluginUniqueID < plugins[j].PluginUniqueID
		})
	case "time-order":
		sort.Slice(plugins, func(i, j int) bool {
			return plugins[i].ModTime.After(plugins[j].ModTime)
		})
	default:
		return false
	}
	return true
}

func pluginUniqueIDs(plugins []scheduler.PluginSnapshot) []string {
	out := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		out = append(out, plugin.PluginUniqueID)
	}
	return out
}

func okListPluginsResp(plugins []string) clientcall.ListPluginsResp {
	if plugins == nil {
		plugins = make([]string, 0)
	}
	return clientcall.ListPluginsResp{
		RespComm: comm.RespComm{
			Code: comm.CodeOK,
			Msg:  comm.GetMsgFromCode(comm.CodeOK),
		},
		Plugins: plugins,
	}
}
