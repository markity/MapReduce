package workercall

import "mapreduce/rpc/comm"

// 200ok response可能是二进制，失败的时候非200,返回json
type FetchPluginByJobIDRespOnFailure struct {
	comm.RespComm
}
