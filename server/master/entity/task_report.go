package entity

type TaskReportStatus string

const (
	TaskReportSucceeded TaskReportStatus = "succeeded"
	TaskReportFailed    TaskReportStatus = "failed"
)

type TaskReport struct {
	TaskAttemptKey
	WorkerUniqueID string
	WorkerAddr     string
	TaskType       TaskType

	// succeed or failed
	Status TaskReportStatus
	Error  string

	// Map任务成功时输出的map attempt位置
	MapOutput *MapOutputMetaEntry
	// Reduce任务，Error!=nil，失败时，可以输出哪些partition拉取失败
	//	master可以重新进行调度map任务
	LostMapOutputs []MapOutputMetaEntry
}

func (r TaskReport) Key() TaskAttemptKey {
	return r.TaskAttemptKey
}
