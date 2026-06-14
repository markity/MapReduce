package clientapis

import (
	"mapreduce/rpc/comm"
)

func badRequestResp() comm.RespComm {
	return comm.RespComm{
		Code: comm.CodeBadRequest,
		Msg:  comm.GetMsgFromCode(comm.CodeBadRequest),
	}
}
