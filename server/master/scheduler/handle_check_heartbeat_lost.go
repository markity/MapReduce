package scheduler

import "time"

func (impl *schedulerImpl) handleCheckHeartbeatLost() {
	now := time.Now()
	for _, worker := range impl.workerStatus {
		if worker.State == workerStateLost {
			continue
		}
		if worker.LastSeen.Add(impl.workerHeartbeatLostInterval).After(now) {
			continue
		}

		// 处理心跳过期
		worker.State = workerStateLost
		impl.requeueAllWorkerRunningAttempts(worker)
	}
}
