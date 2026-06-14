package comm

type TaskAttemptKey struct {
	JobID     string `json:"job_id"`
	TaskID    string `json:"task_id"`
	AttemptID string `json:"attempt_id"`
}

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type TaskSlotStatusReportRunningTask struct {
	TaskAttemptKey
	TaskType TaskType `json:"task_type"`
}

type TaskSlotStatusReport struct {
	SlotID             string                           `json:"slot_id"`
	CurrentRunningTask *TaskSlotStatusReportRunningTask `json:"current_running_task"`
}

type AssignTask struct {
	SlotID string `json:"slot_id"`
	TaskAttemptKey
	TaskType TaskType `json:"task_type"`

	Plugin PluginSpec        `json:"plugin_spec"`
	Conf   map[string]string `json:"conf"`

	// 二选一
	MapTask    *MapTaskSpec    `json:"map_task,omitempty"`
	ReduceTask *ReduceTaskSpec `json:"reduce_task,omitempty"`
}
