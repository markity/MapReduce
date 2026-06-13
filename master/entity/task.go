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
