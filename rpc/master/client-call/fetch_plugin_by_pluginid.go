package clientcall

import "mapreduce/rpc/comm"

// 200 response is plugin binary. Non-200 response is JSON.
type FetchPluginByPluginIDRespOnFailure struct {
	comm.RespComm
}
