package entity

// --- create map reduce job ---
type CreateMapReduceJobCode int

const (
	CreateMapReduceJobCodeOK CreateMapReduceJobCode = iota
	CreateMapReduceJobCodePluginNotFound
)

const (
	CreateMapReduceJobCodeInternalError CreateMapReduceJobCode = CodeInternalError
)

type CreateMapReduceJobInput struct {
	JobName        string
	PluginUniqueID string
	NumReduceTasks int
	TaskSplits     []SplitSpec
}

type CreateMapReduceJobOutput struct {
	Code  CreateMapReduceJobCode
	JobID string
}

// ------

// --- list jobs ---
type ListJobsCode int

const (
	ListJobsCodeOK ListJobsCode = iota
)

const (
	ListJobsCodeInternalError ListJobsCode = CodeInternalError
)

type JobInfo struct {
	JobID   string
	JobName string
	Status  string
}

type ListJobsOutput struct {
	Code ListJobsCode
	Jobs []JobInfo
}

// ------

// --- get job ----
type GetJobCode int

const (
	GetJobCodeOK GetJobCode = iota
	GetJobCodeNotFound
)
const (
	GetJobCodeInternalError GetJobCode = CodeInternalError
)

type GetJobInput struct {
	JobID string
}

type GetJobOutput struct {
	Code GetJobCode
	Job  *JobInfo
}

// ------
