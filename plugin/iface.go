package plugin

type Split interface {
	ID() string
	Kind() string
	Locations() []string
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type InputFormat interface {
	GetSplits(conf Configuration) ([]Split, error)

	NewSplit(kind string) (Split, error)

	CreateRecordReader(
		split Split,
		conf Configuration,
	) (RecordReader, error)
}

type RecordReader interface {
	Next() bool
	Record() (key []byte, value []byte)
	Err() error
	Close() error
}

type MapContext interface {
	Write(key []byte, value []byte) error
	Configuration() Configuration
}

type Mapper interface {
	Map(key []byte, value []byte, ctx MapContext) error
}

type ReduceContext interface {
	Write(key []byte, value []byte) error
	Configuration() Configuration
}

type Reducer interface {
	Reduce(key []byte, values [][]byte, ctx ReduceContext) error
}

type Partitioner interface {
	Partition(key []byte, value []byte, numPartitions int) int
}

type OutputFormat interface {
	// filepath.Join(spec.AttemptDir, "reduce-output")，是一个文件路径
	CreateRecordWriter(outputPath string, conf Configuration) (RecordWriter, error)
	OutputCommitter() OutputCommitter
}

type RecordWriter interface {
	Write(key []byte, value []byte) error
	Close() error
}

type OutputCommitter interface {
	SetupTask(outputPath string, conf Configuration) error
	CommitTask(outputPath string, conf Configuration) error
	AbortTask(outputPath string, conf Configuration) error
}

type JobPlugin interface {
	InputFormat() InputFormat
	Partitioner() Partitioner
	OutputFormat() OutputFormat
	Mapper() Mapper
	Reducer() Reducer
}
