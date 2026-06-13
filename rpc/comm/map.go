package comm

// map执行完毕后输出一个MapOutputMetaEntry，表示该map attempt的输出位置。
// reducer拉取数据时使用该attempt key和自己的ReducePartitionID定位分区数据。
type MapOutputMetaEntry struct {
	TaskAttemptKey

	WorkerUniqueID string `json:"worker_unique_id"`
	WorkerAddr     string `json:"worker_addr"`

	Size int64 `json:"size"`
}
