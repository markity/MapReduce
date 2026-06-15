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
