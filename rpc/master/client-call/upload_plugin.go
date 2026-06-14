package clientcall

import "mapreduce/rpc/comm"

// url: Post /client/upload-plugin/{插件名字}，body为插件binary blob
type UploadPluginResp struct {
	comm.RespComm

	PluginUniqueID string `json:"plugin_unique_id"`
}
