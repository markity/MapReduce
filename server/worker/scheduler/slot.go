package scheduler

import (
	"fmt"
	"mapreduce/rpc/comm"
	"mapreduce/server/worker/entity"
)

type slotStatus struct {
	SlotID             string
	CurrentRunningTask *entity.TaskSlotAssigned
}

func newSlotTable(n int) *slotTable {
	slots := make(map[string]*slotStatus, n)
	for i := 0; i < n; i++ {
		slotID := "slot-" + fmt.Sprint(i)
		slots[slotID] = &slotStatus{SlotID: slotID}
	}
	return &slotTable{slots: slots}
}

type slotTable struct {
	slots map[string]*slotStatus
}

func (t *slotTable) markRunning(assign entity.TaskSlotAssigned) bool {
	slot := t.slots[assign.SlotID]
	if slot == nil || slot.CurrentRunningTask != nil {
		return false
	}
	slot.CurrentRunningTask = &assign
	return true
}

func (t *slotTable) releaseAttempt(key entity.TaskAttemptKey) {
	for _, slot := range t.slots {
		if slot.CurrentRunningTask == nil {
			continue
		}
		if slot.CurrentRunningTask.TaskAttemptKey == key {
			slot.CurrentRunningTask = nil
			return
		}
	}
}

func (t *slotTable) heartbeatSnapshot() map[string]comm.TaskSlotStatusReport {
	slotStatus := make(map[string]comm.TaskSlotStatusReport, len(t.slots))
	for slotID, slot := range t.slots {
		slotStatus[slotID] = slot.heartbeatSnapshot()
	}
	return slotStatus
}

func (s *slotStatus) heartbeatSnapshot() comm.TaskSlotStatusReport {
	if s.CurrentRunningTask == nil {
		return comm.TaskSlotStatusReport{
			SlotID:             s.SlotID,
			CurrentRunningTask: nil,
		}
	}

	entityRunningTask := entity.RunningTaskFromTaskSlotAssigned(s.CurrentRunningTask)
	commRunningTask := entity.RpcCommRunningTaskFromRunningTask(entityRunningTask)
	return comm.TaskSlotStatusReport{
		SlotID:             s.SlotID,
		CurrentRunningTask: commRunningTask,
	}
}
