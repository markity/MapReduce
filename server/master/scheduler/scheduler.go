package scheduler

import (
	"container/list"
	"mapreduce/server/master/entity"
	"time"
)

type Scheduler interface {
	PostHeartbeatReq(*entity.HeartbeatInput) *entity.HeartbeatOutput
	CreateMapReduceJob(*entity.CreateMapReduceJobInput) *entity.CreateMapReduceJobOutput
	ListJobs() *entity.ListJobsOutput
	GetJob(*entity.GetJobInput) *entity.GetJobOutput
	GetMasterState() *entity.GetMasterStateOutput
	// 如果没有job直接返回，也不去pin了, 如果job结束，返回job已经结束的错误代码，此时也不pin
	//	唯一pin的情况就是job还在运行，此时plugin也一定还是没被物理删除的状态，可以pin
	GetJobPluginFilePathAndPin(jobID string, pinSecret *string) GetJobPluginFilePathOutput
	JobPluginFileUnpin(jobID string, pinSecret string) bool
	GetPluginFilePathAndPin(pluginUniqueID string, pinSecret *string) GetPluginFilePathOutput
	PluginFileUnpin(pluginUniqueID string, pinSecret string) bool
	RegisterPlugin(string) string
	DeletePlugin(string) (bool, error)
	ListPlugins() []PluginSnapshot
	CleanupDeletedPlugins() error
}

// 存储每个worker的状态，每个job的调度信息
type schedulerImpl struct {
	// recv heartbeat req from worker, and send back heartbeat resp
	heartbeatReqInputChan chan *heartbeatReqInput

	// 增加/列表/详细任务相关
	createJobInputChan chan *createMapReduceJobInput
	listJobsInputChan  chan *listJobsInput
	getJobInputChan    chan *getJobInput
	getMasterStateChan chan *getMasterStateInput

	// 获得job的filepath，用于fetch plugin blob接口使用
	getJobPluginFilePathAndPinChan chan *getJobPluginFilePathAndPinInput
	// unpin plugin
	unpinJobPluginChan          chan *unpinJobPluginInput
	getPluginFilePathAndPinChan chan *getPluginFilePathAndPinInput
	unpinPluginChan             chan *unpinPluginInput
	// 注册新的plugin
	registerPluginChan chan *registerPluginInput
	// 删除已有的plugin
	deletePluginChan chan *deletePluginInput
	// 列表plugin
	listPluginsChan chan *listPluginsInput
	// 清除之前软删除的插件
	cleanupPluginsChan chan *cleanupPluginsInput

	// job 连续自增，jobid生成器
	nextJobSeq uint64
	// attempt 连续自增，attempt id生成器
	nextAttemptSeq uint64

	workerHeartbeatLostInterval time.Duration
	pluginStorePath             string

	// 下面是状态字段

	// worker unique id -> workerstatus
	workerStatus map[string]*workerStatus
	// jobid -> jobstatus
	jobStatus map[string]*jobStatus
	// runnable job queue, stores job id as string
	runnableJobQueue *list.List
	runnableJobIndex map[string]*list.Element
	// plugin unique id -> plugin status
	pluginStatus map[string]*pluginStatus
}

var schedulerInstance *schedulerImpl

func GetScheduler() Scheduler {
	if schedulerInstance == nil {
		panic("failed to get scheduler")
	}
	return schedulerInstance
}

func InitScheduler(plugins []pluginStatus, workerHeartbeatLostIntervalSeconds int, pluginStorePath string) Scheduler {
	schedulerInstance = newSchedulerImpl(plugins, workerHeartbeatLostIntervalSeconds, pluginStorePath)
	return schedulerInstance
}

func newSchedulerImpl(plugins []pluginStatus, workerHeartbeatLostIntervalSeconds int, pluginStorePath string) *schedulerImpl {
	impl := &schedulerImpl{
		heartbeatReqInputChan:          make(chan *heartbeatReqInput),
		createJobInputChan:             make(chan *createMapReduceJobInput),
		listJobsInputChan:              make(chan *listJobsInput),
		getJobInputChan:                make(chan *getJobInput),
		getMasterStateChan:             make(chan *getMasterStateInput),
		getJobPluginFilePathAndPinChan: make(chan *getJobPluginFilePathAndPinInput),
		unpinJobPluginChan:             make(chan *unpinJobPluginInput),
		getPluginFilePathAndPinChan:    make(chan *getPluginFilePathAndPinInput),
		unpinPluginChan:                make(chan *unpinPluginInput),
		registerPluginChan:             make(chan *registerPluginInput),
		deletePluginChan:               make(chan *deletePluginInput),
		listPluginsChan:                make(chan *listPluginsInput),
		cleanupPluginsChan:             make(chan *cleanupPluginsInput),
		workerHeartbeatLostInterval:    time.Second * time.Duration(workerHeartbeatLostIntervalSeconds),
		pluginStorePath:                pluginStorePath,
		workerStatus:                   make(map[string]*workerStatus),
		jobStatus:                      make(map[string]*jobStatus),
		runnableJobQueue:               list.New(),
		runnableJobIndex:               make(map[string]*list.Element),
		pluginStatus:                   pluginStatusMapFromSlice(plugins),
		nextJobSeq:                     10000,
		nextAttemptSeq:                 10000,
	}
	go impl.runLoopForever()
	return impl
}

func (impl *schedulerImpl) runLoopForever() {
	ticker := time.NewTicker(impl.workerHeartbeatLostInterval)

	for {
		select {
		case heartbeatInput := <-impl.heartbeatReqInputChan:
			impl.handleHeartbeatInput(heartbeatInput)
		case <-ticker.C:
			impl.handleCheckHeartbeatLost()
		case createJobInput := <-impl.createJobInputChan:
			impl.handleCreateMapReduceJobInput(createJobInput)
		case listJobsInput := <-impl.listJobsInputChan:
			impl.handleListJobsInput(listJobsInput)
		case getJobInput := <-impl.getJobInputChan:
			impl.handleGetJobInput(getJobInput)
		case getMasterStateInput := <-impl.getMasterStateChan:
			impl.handleGetMasterStateInput(getMasterStateInput)
		case getJobPluginFilePathInput := <-impl.getJobPluginFilePathAndPinChan:
			impl.handleGetJobPluginFilePathInput(getJobPluginFilePathInput)
		case unpinInput := <-impl.unpinJobPluginChan:
			impl.handleUnpinJobPluginInput(unpinInput)
		case getPluginFilePathInput := <-impl.getPluginFilePathAndPinChan:
			impl.handleGetPluginFilePathInput(getPluginFilePathInput)
		case unpinPluginInput := <-impl.unpinPluginChan:
			impl.handleUnpinPluginInput(unpinPluginInput)
		case registerPluginInput := <-impl.registerPluginChan:
			impl.handleRegisterPluginInput(registerPluginInput)
		case deletePluginInput := <-impl.deletePluginChan:
			impl.handleDeletePluginInput(deletePluginInput)
		case listPluginsInput := <-impl.listPluginsChan:
			impl.handleListPluginsInput(listPluginsInput)
		case cleanupPluginsInput := <-impl.cleanupPluginsChan:
			impl.handleCleanupPluginsInput(cleanupPluginsInput)
		}
	}
}
