package entity

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type JobStageCode string

const (
	JobStageCodeMapping   JobStageCode = "mapping"
	JobStageCodeReducing  JobStageCode = "reducing"
	JobStageCodeSucceeded JobStageCode = "succeed"
	JobStageCodeKilled    JobStageCode = "killed"
)

type TaskAttemptKey struct {
	JobID     string
	TaskID    string
	AttemptID string
}

type TaskSlotAssigned struct {
	TaskAttemptKey
	TaskType TaskType
}

type TaskSlotStatus struct {
	SlotID             string
	CurrentRunningTask *TaskSlotAssigned
}
