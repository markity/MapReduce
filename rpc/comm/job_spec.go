package comm

type JobSpec struct {
	JobID string `json:"job_id"`

	Plugin PluginSpec `json:"plugin"`

	Conf map[string]string `json:"conf"`

	InputFormatName string `json:"input_format_name"`
	MapperName      string `json:"mapper_name"`
	ReducerName     string `json:"reducer_name"`
	PartitionerName string `json:"partitioner_name"`

	NumReduce  int    `json:"num_reduce"`
	InputPath  string `json:"input_path"`
	OutputPath string `json:"output_path"`
}
