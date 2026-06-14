package comm

type Code int

const (
	CodeOK = iota

	// client upload-plugin接口
	CodeUploadPluginPluginNameInvalid

	// delete plugin接口
	CodeDeletePluginPluginNotFound

	// fetch-job-plugin接口
	CodeFetchJobPluginJobNotFound
	CodeFetchJobPluginJobTerminated
	CodeFetchPluginPluginNotFound

	// create job接口
	CodeCreateJobPluginNotFound

	// get job接口
	CodeGetJobJobNotFound

	CodeHeartbeatSeqBackoffRequest
	CodeHeartbeatEpochStaleRequest

	CodeInternalError = -1
	CodeBadRequest    = -2
)

var codeToMsg map[Code]string = map[Code]string{
	CodeOK: "ok",

	CodeUploadPluginPluginNameInvalid: "plugin name is invalid",

	CodeDeletePluginPluginNotFound: "plugin not found",

	CodeFetchJobPluginJobNotFound:   "job not found",
	CodeFetchJobPluginJobTerminated: "job terminated",
	CodeFetchPluginPluginNotFound:   "plugin not found",

	CodeCreateJobPluginNotFound: "plugin is not found for create job",

	CodeGetJobJobNotFound: "job not found",

	CodeHeartbeatSeqBackoffRequest: "sequence backoff, this request is ignored",
	CodeHeartbeatEpochStaleRequest: "epoch stale, this is abnormal", // epoch随着机器启动应当是增加的

	CodeInternalError: "internal error",
	CodeBadRequest:    "bad request, check your params",
}

func GetMsgFromCode(c Code) string {
	return codeToMsg[c]
}

type RespComm struct {
	Code Code   `json:"code"`
	Msg  string `json:"msg"`
}
