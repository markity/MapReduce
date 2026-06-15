package runner

import (
	mrplugin "mapreduce/plugin"
	"mapreduce/server/worker/entity"
)

type TaskSpec struct {
	Assign     entity.TaskSlotAssigned `json:"assign"`
	AttemptDir string                  `json:"attempt_dir"`
	ReportPath string                  `json:"report_path"`
	// 若插件在master处，需向master拉取plugin
	MasterAddr string `json:"master_addr"`
	// 这些信息抄写进入task_report文件中
	WorkerUniqueID      string `json:"worker_unique_id"`
	WorkerAddr          string `json:"worker_addr"`
	MapSpillBufferBytes int64  `json:"map_spill_buffer_bytes"`
}

type loadedPlugin struct {
	Conf   mrplugin.Configuration
	Plugin mrplugin.JobPlugin
}
