package entity

type HeartbeatOutputCode int

const (
	HeartbeatOutputCodeOK HeartbeatOutputCode = iota
	HeartbeatOutputCodeSeqBackoffRequest
	HeartbeatOutputCodeEpochStaleRequest
)

const (
	HeartbeatOutputCodeInternalError HeartbeatOutputCode = CodeInternalError
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

type HeartbeatInput struct {
	WorkerUniqueID string
	WorkerEpoch    int64
	WorkerAddr     string
	Seq            uint64

	SlotStatus        map[string]TaskSlotStatus
	Reports           []TaskReport
	CleanupWantedJobs []string
}

type HeartbeatOutput struct {
	Code HeartbeatOutputCode

	AckedReports       []TaskAttemptKey
	CleanableJobs      []string
	AssignedTasks      []AssignedTask
	ShouldStopAttempts []TaskAttemptKey
}
