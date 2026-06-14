package clientapis

import (
	"encoding/json"
	"mapreduce/master/entity"
	"strings"
	"testing"
)

func TestMasterStateEmptyWorkerSlotListsEncodeAsArrays(t *testing.T) {
	resp := getMasterStateRespFromEntity(&entity.GetMasterStateOutput{
		Code: entity.GetMasterStateCodeOK,
		Workers: []entity.MasterStateWorkerInfo{
			{
				WorkerUniqueID: "worker-1",
				FreeSlots:      []string{},
				BusySlots:      []string{},
			},
		},
	})

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal master state resp: %v", err)
	}
	body := string(data)
	if !strings.Contains(body, `"free_slots":[]`) {
		t.Fatalf("free_slots should encode as [], got %s", body)
	}
	if !strings.Contains(body, `"busy_slots":[]`) {
		t.Fatalf("busy_slots should encode as [], got %s", body)
	}
}
