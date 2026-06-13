package workerapis

import (
	"fmt"
	"log"
	"mapreduce/master/scheduler"
	"mapreduce/rpc/comm"
	workercall "mapreduce/rpc/master/worker-call"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func FetchPlugin() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		if jobID == "" {
			c.JSON(http.StatusBadRequest, fetchPluginFailureResp(comm.CodeBadRequest))
			return
		}

		fetched := false
		var pin string
		output := scheduler.GetScheduler().GetJobPluginFilePathAndPin(jobID, &pin)
		defer func() {
			if fetched && scheduler.GetScheduler().JobPluginFileUnpin(jobID, pin) != true {
				log.Println("warn: failed to unpin, check code")
			}
		}()

		switch output.Code {
		case scheduler.GetJobPluginFilePathAndPinCodeOK:
			fetched = true
		case scheduler.GetJobPluginFilePathCodeJobAndPinNotFound:
			c.JSON(http.StatusNotFound, fetchPluginFailureResp(comm.CodeFetchJobPluginJobNotFound))
			return
		case scheduler.GetJobPluginFilePathCodeJobAndPinTerminated:
			c.JSON(http.StatusGone, fetchPluginFailureResp(comm.CodeFetchJobPluginJobTerminated))
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

		// 因为文件还存在且受pin保护，所以这里不会出错
		c.Header("Content-Length", fmt.Sprint(s.Size()))
		c.Header("Content-Type", "application/octet-stream")
		c.File(output.FilePath)
	}
}

func fetchPluginFailureResp(code comm.Code) workercall.FetchPluginRespOnFailure {
	return workercall.FetchPluginRespOnFailure{
		RespComm: comm.RespComm{
			Code: code,
			Msg:  comm.GetMsgFromCode(code),
		},
	}
}
