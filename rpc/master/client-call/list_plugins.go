package clientcall

import "mapreduce/rpc/comm"

// url: Get /client/plugins/{dict-order/time-order}
type ListPluginsResp struct {
	comm.RespComm

	Plugins     []string                         `json:"plugins"` // plugin unique id list
	PluginInfos []ListPluginsRespPluginInfoEntry `json:"plugin_infos"`
}

type ListPluginsRespPluginInfoEntry struct {
	PluginUniqueID string `json:"plugin_unique_id"`
	UploadedAt     string `json:"uploaded_at"`
}
