package scheduler

import (
	"mapreduce/master/entity"
	"testing"
)

func newRegressionScheduler() *schedulerImpl {
	return newSchedulerImpl(nil, 10)
}

func createRegressionJob(t *testing.T, impl *schedulerImpl, splits int, reduces int) *entity.CreateMapReduceJobOutput {
	t.Helper()
	pluginUniqueID := "plugin-regression"
	impl.pluginStatus[pluginUniqueID] = &pluginStatus{
		PluginUniqueID: pluginUniqueID,
		FilePath:       "/tmp/plugin-regression",
		Pin:            make(map[string]struct{}),
	}
	taskSplits := make([]entity.SplitSpec, 0, splits)
	for i := 0; i < splits; i++ {
		taskSplits = append(taskSplits, entity.SplitSpec{SplitType: "test"})
	}
	resp := impl.CreateMapReduceJob(&entity.CreateMapReduceJobInput{
		JobName:        "regression",
		PluginUniqueID: pluginUniqueID,
		NumReduceTasks: reduces,
		TaskSplits:     taskSplits,
	})
	if resp.Code != entity.CreateMapReduceJobCodeOK {
		t.Fatalf("create job code = %v, want %v", resp.Code, entity.CreateMapReduceJobCodeOK)
	}
	return resp
}

func TestCreateJobInitializesPendingAndMapOutputIndexes(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 2, 1)
	job := impl.jobStatus[resp.JobID]

	if len(job.PendingMapTasks) != 2 {
		t.Fatalf("pending map tasks len = %d, want 2", len(job.PendingMapTasks))
	}
	if len(job.PendingReduceTasks) != 1 {
		t.Fatalf("pending reduce tasks len = %d, want 1", len(job.PendingReduceTasks))
	}
	if len(job.AllAvailableMapOutputs) != 2 {
		t.Fatalf("available map output indexes len = %d, want 2", len(job.AllAvailableMapOutputs))
	}
	if job.allMapTasksDone() {
		t.Fatalf("new job should not have all map tasks done")
	}

	heartbeat := impl.PostHeartbeatReq(&entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    1,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            1,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
		},
	})
	if len(heartbeat.AssignedTasks) != 1 {
		t.Fatalf("assigned tasks len = %d, want 1", len(heartbeat.AssignedTasks))
	}
	assigned := heartbeat.AssignedTasks[0]
	if len(job.InFlightMapTasks[assigned.TaskID]) != 1 {
		t.Fatalf("in-flight attempts for %s len = %d, want 1", assigned.TaskID, len(job.InFlightMapTasks[assigned.TaskID]))
	}
}

func TestFinishInFlightAttemptKeepsSiblingAttempt(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]
	task := job.AllMapTasks["map-0"]
	first := makeTaskAttemptKeyFromTaskStatus(task, "attempt-1")
	second := makeTaskAttemptKeyFromTaskStatus(task, "attempt-2")

	job.markAttemptKeyInFlight(first)
	job.markAttemptKeyInFlight(second)
	if !job.finishInFlightByAttemptKey(first) {
		t.Fatalf("finish first attempt returned false")
	}
	if _, ok := job.InFlightMapTasks["map-0"][second]; !ok {
		t.Fatalf("second attempt should remain in-flight")
	}
	if _, ok := job.PendingMapTasks["map-0"]; ok {
		t.Fatalf("map task should not become pending while sibling attempt is in-flight")
	}
}

func TestLostAttemptKeepsSiblingAttemptAndOnlyRequeuesWhenAllAttemptsGone(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]
	task := job.AllMapTasks["map-0"]
	first := makeTaskAttemptKeyFromTaskStatus(task, "attempt-1")
	second := makeTaskAttemptKeyFromTaskStatus(task, "attempt-2")

	job.markAttemptKeyInFlight(first)
	job.markAttemptKeyInFlight(second)
	impl.rescheduleLostAttempt(first)
	if _, ok := job.InFlightMapTasks["map-0"][second]; !ok {
		t.Fatalf("second attempt should remain in-flight after first is lost")
	}
	if _, ok := job.PendingMapTasks["map-0"]; ok {
		t.Fatalf("map task should not be pending while second attempt is still in-flight")
	}

	impl.rescheduleLostAttempt(second)
	if len(job.InFlightMapTasks["map-0"]) != 0 {
		t.Fatalf("in-flight attempts len = %d, want 0", len(job.InFlightMapTasks["map-0"]))
	}
	if _, ok := job.PendingMapTasks["map-0"]; !ok {
		t.Fatalf("map task should be pending after all attempts are lost")
	}
}

func TestMapSuccessStopsSiblingAttemptAndReduceGetsMapOutputs(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]
	task := job.AllMapTasks["map-0"]
	winner := makeTaskAttemptKeyFromTaskStatus(task, "attempt-1")
	loser := makeTaskAttemptKeyFromTaskStatus(task, "attempt-2")
	job.markAttemptKeyInFlight(winner)
	job.markAttemptKeyInFlight(loser)

	acked := impl.handleTaskReportIdempotently("worker-1", entity.TaskReport{
		TaskAttemptKey: winner,
		WorkerUniqueID: "worker-1",
		WorkerAddr:     "127.0.0.1:9000",
		TaskType:       entity.TaskTypeMap,
		Status:         entity.TaskReportSucceeded,
		MapOutput: &entity.MapOutputMetaEntry{
			WorkerUniqueID: "worker-1",
			WorkerAddr:     "127.0.0.1:9000",
			TaskAttemptKey: winner,
			Size:           10,
		},
	})
	if !acked {
		t.Fatalf("successful map report should be acked")
	}
	if len(job.InFlightMapTasks["map-0"]) != 0 {
		t.Fatalf("sibling attempts should be cleared after first success")
	}
	if len(job.AllAvailableMapOutputs["map-0"]) != 1 {
		t.Fatalf("available map outputs len = %d, want 1", len(job.AllAvailableMapOutputs["map-0"]))
	}
	if job.JobStage != entity.JobStageCodeReducing {
		t.Fatalf("job stage = %q, want reducing", job.JobStage)
	}

	pickedJob, reduce := impl.pickPendingTask()
	if pickedJob != job || reduce == nil || reduce.TaskType != entity.TaskTypeReduce {
		t.Fatalf("picked job/task = %p/%+v, want reduce task for job", pickedJob, reduce)
	}
	if reduce.ReduceTaskSpec == nil || len(reduce.ReduceTaskSpec.MapOutputs) != 1 {
		t.Fatalf("reduce task map outputs = %+v, want one map output", reduce.ReduceTaskSpec)
	}
	assigned := impl.buildAssignedTask(makeTaskAttemptKeyFromTaskStatus(reduce, "attempt-reduce"), "slot-1", reduce)
	if assigned.ReduceTask == nil || len(assigned.ReduceTask.MapOutputs) != 1 {
		t.Fatalf("assigned reduce map outputs = %+v, want one map output", assigned.ReduceTask)
	}
}

func TestWorkerRestartRequeuesRunningAttemptsWithoutPanic(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]

	first := impl.PostHeartbeatReq(&entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    1,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            1,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
		},
	})
	if len(first.AssignedTasks) != 1 {
		t.Fatalf("assigned tasks len = %d, want 1", len(first.AssignedTasks))
	}
	oldAttempt := first.AssignedTasks[0].TaskAttemptKey

	second := impl.PostHeartbeatReq(&entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    2,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            2,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
		},
	})
	if second.Code != entity.HeartbeatOutputCodeOK {
		t.Fatalf("heartbeat code = %v, want ok", second.Code)
	}
	if _, ok := job.InFlightMapTasks[oldAttempt.TaskID][oldAttempt]; ok {
		t.Fatalf("old attempt still exists in-flight after worker restart")
	}
}

func TestDuplicateHeartbeatSeqIsRejected(t *testing.T) {
	impl := newRegressionScheduler()
	_ = createRegressionJob(t, impl, 1, 1)
	req := &entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    1,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            1,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
		},
	}
	first := impl.PostHeartbeatReq(req)
	if first.Code != entity.HeartbeatOutputCodeOK {
		t.Fatalf("first heartbeat code = %v, want ok", first.Code)
	}
	second := impl.PostHeartbeatReq(req)
	if second.Code != entity.HeartbeatOutputCodeSeqBackoffRequest {
		t.Fatalf("duplicate heartbeat code = %v, want seq backoff", second.Code)
	}
	if len(second.AssignedTasks) != 0 {
		t.Fatalf("duplicate heartbeat assigned tasks len = %d, want 0", len(second.AssignedTasks))
	}
}

func TestMissingReportedSlotIsNotAssignable(t *testing.T) {
	impl := newRegressionScheduler()
	_ = createRegressionJob(t, impl, 3, 1)

	first := impl.PostHeartbeatReq(&entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    1,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            1,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
			"slot-2": {SlotID: "slot-2"},
		},
	})
	if len(first.AssignedTasks) != 2 {
		t.Fatalf("first assigned tasks len = %d, want 2", len(first.AssignedTasks))
	}

	second := impl.PostHeartbeatReq(&entity.HeartbeatInput{
		WorkerUniqueID: "worker-1",
		WorkerEpoch:    1,
		WorkerAddr:     "127.0.0.1:9000",
		Seq:            2,
		SlotStatus: map[string]entity.TaskSlotStatus{
			"slot-1": {SlotID: "slot-1"},
		},
	})
	if len(second.AssignedTasks) != 1 {
		t.Fatalf("second assigned tasks len = %d, want 1", len(second.AssignedTasks))
	}
	if second.AssignedTasks[0].SlotID != "slot-1" {
		t.Fatalf("assigned slot = %q, want slot-1", second.AssignedTasks[0].SlotID)
	}
	if _, ok := impl.workerStatus["worker-1"].FreeSlots["slot-2"]; ok {
		t.Fatalf("missing reported slot-2 should not be free/assignable")
	}
}

func TestInvalidMapSuccessReportRequeuesMapTask(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]
	task := job.AllMapTasks["map-0"]
	attempt := makeTaskAttemptKeyFromTaskStatus(task, "attempt-1")
	job.markAttemptKeyInFlight(attempt)

	acked := impl.handleTaskReportIdempotently("worker-1", entity.TaskReport{
		TaskAttemptKey: attempt,
		WorkerUniqueID: "worker-1",
		WorkerAddr:     "127.0.0.1:9000",
		TaskType:       entity.TaskTypeMap,
		Status:         entity.TaskReportSucceeded,
		MapOutput:      nil,
	})
	if !acked {
		t.Fatalf("invalid map success report should still be acked")
	}
	if len(job.AllAvailableMapOutputs["map-0"]) != 0 {
		t.Fatalf("invalid map output should not be recorded")
	}
	if _, ok := job.PendingMapTasks["map-0"]; !ok {
		t.Fatalf("map task should be pending after invalid map output report")
	}
	if task.TaskState != TaskRunStatePending {
		t.Fatalf("map task state = %q, want pending", task.TaskState)
	}
}

func TestPluginPinCanUnpinAfterJobTerminal(t *testing.T) {
	impl := newRegressionScheduler()
	resp := createRegressionJob(t, impl, 1, 1)
	job := impl.jobStatus[resp.JobID]

	var secret string
	out := impl.GetJobPluginFilePathAndPin(resp.JobID, &secret)
	if out.Code != GetJobPluginFilePathAndPinCodeOK {
		t.Fatalf("pin code = %v, want ok", out.Code)
	}
	if secret == "" {
		t.Fatalf("pin secret is empty")
	}
	if len(impl.pluginStatus[job.PluginUniqueID].Pin) != 1 {
		t.Fatalf("pin len = %d, want 1", len(impl.pluginStatus[job.PluginUniqueID].Pin))
	}
	job.JobStage = entity.JobStageCodeSucceeded
	if !impl.JobPluginFileUnpin(resp.JobID, secret) {
		t.Fatalf("unpin should succeed after job becomes terminal")
	}
	if len(impl.pluginStatus[job.PluginUniqueID].Pin) != 0 {
		t.Fatalf("pin len after unpin = %d, want 0", len(impl.pluginStatus[job.PluginUniqueID].Pin))
	}
}
