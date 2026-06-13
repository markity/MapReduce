package comm

type MapTaskSpec struct {
	NumReduce int       `json:"num_reduce"`
	Split     SplitSpec `json:"split"`
}

type ReduceTaskSpec struct {
	ReducerName       string               `json:"reducer_name"`
	OutputFormatName  string               `json:"output_format_name"`
	ReducePartitionID int                  `json:"reduce_partition_id"`
	MapOutputs        []MapOutputMetaEntry `json:"map_outputs"`
}
