package clientcall

import "mapreduce/rpc/comm"

// url: Delete /client/plugin/{pluginUniqueID}
type DeletePluginResp struct {
	comm.RespComm
}
