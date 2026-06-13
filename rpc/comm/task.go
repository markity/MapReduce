package comm

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type RunningTask struct {
	TaskAttemptKey
	TaskType TaskType `json:"task_type"`
}

type TaskSlotStatus struct {
	SlotID             string       `json:"slot_id"`
	CurrentRunningTask *RunningTask `json:"current_running_task"`
}
