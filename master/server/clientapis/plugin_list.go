package clientapis

import (
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	clientcall "mapreduce/rpc/master/client-call"
	"net/http"
	"sort"
	"time"

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
		c.JSON(http.StatusOK, okListPluginsResp(plugins))
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

func pluginNames(plugins []scheduler.PluginSnapshot) []string {
	out := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		out = append(out, plugin.PluginUniqueID)
	}
	return out
}

func okListPluginsResp(plugins []scheduler.PluginSnapshot) clientcall.ListPluginsResp {
	names := pluginNames(plugins)
	pluginInfos := pluginInfosFromSnapshots(plugins)
	if names == nil {
		names = make([]string, 0)
	}
	if pluginInfos == nil {
		pluginInfos = make([]clientcall.PluginInfo, 0)
	}
	return clientcall.ListPluginsResp{
		RespComm: comm.RespComm{
			Code: comm.CodeOK,
			Msg:  comm.GetMsgFromCode(comm.CodeOK),
		},
		Plugins:     names,
		PluginInfos: pluginInfos,
	}
}

func pluginInfosFromSnapshots(plugins []scheduler.PluginSnapshot) []clientcall.PluginInfo {
	out := make([]clientcall.PluginInfo, 0, len(plugins))
	for _, plugin := range plugins {
		out = append(out, clientcall.PluginInfo{
			PluginUniqueID: plugin.PluginUniqueID,
			UploadedAt:     plugin.ModTime.Format(time.RFC3339Nano),
		})
	}
	return out
}
