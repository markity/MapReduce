package comm

type TaskReportStatus string

const (
	TaskReportSucceeded TaskReportStatus = "succeeded"
	TaskReportFailed    TaskReportStatus = "failed"
)

type TaskReport struct {
	TaskAttemptKey
	TaskType TaskType         `json:"task_type"`
	Status   TaskReportStatus `json:"status"`

	Error string `json:"error,omitempty"`

	MapOutput      *MapOutputMetaEntry  `json:"map_output,omitempty"`
	LostMapOutputs []MapOutputMetaEntry `json:"lost_map_outputs"`
}

func (r TaskReport) Key() TaskAttemptKey {
	return r.TaskAttemptKey
}
