package scheduler

import (
	"log"
	"mapreduce/rpc/comm"
	workercall "mapreduce/rpc/master/worker-call"
	"mapreduce/server/worker/entity"
	"time"
)

type Scheduler interface {
	Run()
}

type schedulerImpl struct {
	Cfg             Config
	SlotTable       *slotTable
	Client          masterClient
	Seq             uint64
	HeartbeatTicker *time.Ticker
	Reports         *reportQueue
	Processes       map[entity.TaskAttemptKey]*taskProcess
	ProcessResults  chan taskProcessResult

	// hang住的map attempt，之后需要清理文件资源，需要反复询问master
	//	这个attempt对应的job结束没有，结束了才删除文件
	UncleanedMapHangAttempts map[entity.TaskAttemptKey]struct{}
}

func NewScheduer(cfg Config) Scheduler {
	cfg = cfg.normalized()
	impl := schedulerImpl{
		Cfg:                      cfg,
		SlotTable:                newSlotTable(cfg.NumSlots),
		Client:                   newHTTPMasterClient(cfg.MasterAddr),
		Seq:                      0,
		HeartbeatTicker:          nil,
		Reports:                  newReportQueue(),
		Processes:                make(map[entity.TaskAttemptKey]*taskProcess),
		ProcessResults:           make(chan taskProcessResult, cfg.NumSlots),
		UncleanedMapHangAttempts: make(map[entity.TaskAttemptKey]struct{}),
	}
	return &impl
}

func (impl *schedulerImpl) fillHeartbeatRequest() workercall.HeartbeatReq {
	slotStatus := impl.SlotTable.heartbeatSnapshot()

	reportSnapshots := impl.Reports.Snapshot()
	reports := make([]comm.TaskReport, 0, len(reportSnapshots))
	for _, report := range reportSnapshots {
		commReport := entity.RpcCommTaskReportFromEntity(report)
		reports = append(reports, commReport)
	}

	cleanupWantedJobsSet := make(map[string]struct{})
	for attempt := range impl.UncleanedMapHangAttempts {
		if attempt.JobID == "" {
			log.Printf("warn: skip cleanup wanted attempt with empty job id: %+v", attempt)
			continue
		}
		cleanupWantedJobsSet[attempt.JobID] = struct{}{}
	}
	cleanupWantedJobs := make([]string, 0, len(cleanupWantedJobsSet))
	for jobID := range cleanupWantedJobsSet {
		cleanupWantedJobs = append(cleanupWantedJobs, jobID)
	}

	result := workercall.HeartbeatReq{
		WorkerUniqueID:    impl.Cfg.WorkerUniqueID,
		WorkerEpoch:       impl.Cfg.WorkerEpoch,
		WorkerAddr:        impl.Cfg.AdvertiseAddr,
		Seq:               impl.Seq,
		SlotStatus:        slotStatus,
		Reports:           reports,
		CleanupWantedJobs: cleanupWantedJobs,
	}
	impl.Seq++
	return result
}

func (impl *schedulerImpl) Run() {
	defer impl.killAllProcesses()
	if err := impl.startMapOutputServer(); err != nil {
		log.Printf("start map output server failed: %v", err)
	}
	impl.HeartbeatTicker = time.NewTicker(impl.Cfg.HeartbeatInterval)
	for range impl.HeartbeatTicker.C {
		impl.drainProcessResults()
		resp, err := impl.Client.Heartbeat(impl.fillHeartbeatRequest())
		if err != nil {
			log.Printf("heartbeat failed: resp=%+v err=%v", resp, err)
			continue
		}
		impl.handleHeartbeatResponse(resp)
	}
}

func (impl *schedulerImpl) handleHeartbeatResponse(resp *workercall.HeartbeatResp) {
	if resp == nil {
		return
	}
	if !okCode(resp.Code) {
		return
	}
	impl.Reports.Ack(entity.TaskAttemptKeysFromRpcComm(resp.AckedReports))
	impl.handleCleanableJobs(resp.CleanableJobs)
	impl.handleShouldStopAttempts(entity.TaskAttemptKeysFromRpcComm(resp.ShouldStopAttempts))
	impl.handleAssignedTasks(resp.AssignedTasks)
}
