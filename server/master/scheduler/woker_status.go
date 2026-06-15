package scheduler

import (
	"log"
	"mapreduce/server/master/entity"
	"time"
)

type taskSlotAssigned struct {
	TaskType entity.TaskType
	entity.TaskAttemptKey
}

type slotStatus struct {
	SlotID             string
	CurrentRunningTask *taskSlotAssigned
}

type workerState string

const (
	workerStateAlive workerState = "alive"
	workerStateLost  workerState = "lost"
)

// worker status
type workerStatus struct {
	State    workerState
	LastSeen time.Time

	UniqueID  string
	Addr      string
	Epoch     int64
	LastSeq   uint64
	HasSeq    bool
	AllSlots  map[string]*slotStatus
	FreeSlots map[string]struct{}
	BusySlots map[string]struct{}
}

func makeTaskAttemptKeyFromTaskSlotAssigned(task *taskSlotAssigned) entity.TaskAttemptKey {
	return makeTaskAttemptKey(task.JobID, task.TaskID, task.AttemptID)
}

func newWorkerStatusFromHeartbeat(workerUniqueID string, workerAddr string, workerEpoch int64) *workerStatus {
	worker := &workerStatus{
		LastSeen: time.Now(),

		LastSeq: 0,
		HasSeq:  false,

		UniqueID:  workerUniqueID,
		Addr:      workerAddr,
		Epoch:     workerEpoch,
		AllSlots:  make(map[string]*slotStatus),
		FreeSlots: make(map[string]struct{}),
		BusySlots: make(map[string]struct{}),
		State:     workerStateAlive,
	}
	return worker
}

func taskSlotAssignedFromEntity(task *entity.TaskSlotAssigned) *taskSlotAssigned {
	if task == nil {
		return nil
	}
	return &taskSlotAssigned{
		TaskType:       task.TaskType,
		TaskAttemptKey: makeTaskAttemptKey(task.JobID, task.TaskID, task.AttemptID),
	}
}

// func (w *workerStatus) ensureSlotIndexes() {
// 	if w.AllSlots == nil {
// 		w.AllSlots = make(map[string]*slotStatus)
// 	}
// 	if w.FreeSlots == nil {
// 		w.FreeSlots = make(map[string]struct{})
// 	}
// 	if w.BusySlots == nil {
// 		w.BusySlots = make(map[string]struct{})
// 	}
// }

func (w *workerStatus) ensureSlot(slotID string) *slotStatus {
	slot := w.AllSlots[slotID]
	if slot == nil {
		slot = &slotStatus{SlotID: slotID}
		w.AllSlots[slotID] = slot
	}
	return slot
}

func (w *workerStatus) resetSlotAvailability() {
	w.FreeSlots = make(map[string]struct{})
	w.BusySlots = make(map[string]struct{})
}

func (w *workerStatus) forgetReportedSlotAvailability() {
	w.resetSlotAvailability()
	for _, slot := range w.AllSlots {
		slot.CurrentRunningTask = nil
	}
}

func (w *workerStatus) markSlotFree(slotID string) {
	slot := w.ensureSlot(slotID)
	slot.CurrentRunningTask = nil
	delete(w.BusySlots, slotID)
	w.FreeSlots[slotID] = struct{}{}
}

func (w *workerStatus) markSlotBusy(slotID string, taskType entity.TaskType, attemptKey entity.TaskAttemptKey) {
	slot := w.ensureSlot(slotID)
	slot.CurrentRunningTask = &taskSlotAssigned{
		TaskAttemptKey: attemptKey,
		TaskType:       taskType,
	}
	delete(w.FreeSlots, slotID)
	w.BusySlots[slotID] = struct{}{}
}

func (w *workerStatus) rebuildSlotSnapshot(slots map[string]entity.TaskSlotStatus) {
	w.forgetReportedSlotAvailability()
	for _, slot := range slots {
		taskAssign := taskSlotAssignedFromEntity(slot.CurrentRunningTask)
		if taskAssign == nil {
			w.markSlotFree(slot.SlotID)
			continue
		}
		w.markSlotBusy(slot.SlotID, taskAssign.TaskType, taskAssign.TaskAttemptKey)
	}
}

func (w *workerStatus) releaseTask(key entity.TaskAttemptKey) {
	for slotID, slot := range w.AllSlots {
		if slotID != slot.SlotID {
			log.Println("warn: releaseTask: slot id != slot.SlotID")
		}
		if slot.CurrentRunningTask == nil {
			continue
		}
		task := slot.CurrentRunningTask
		if task.JobID == key.JobID && task.TaskID == key.TaskID && task.AttemptID == key.AttemptID {
			w.markSlotFree(slotID)
		}
	}
}
