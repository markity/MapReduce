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

	CreateRecordReader(
		split Split,
		conf Configuration,
	) (RecordReader, error)
}

type SplitFactory interface {
	NewSplit(kind string) (Split, error)
}

type RecordReader interface {
	Next() bool
	Key() string
	Value() string
	Err() error
	Close() error
}

type MapContext interface {
	Write(key []byte, value []byte) error
	Configuration() Configuration
}

type Mapper interface {
	Map(key string, value string, ctx MapContext) error
}

type ReduceContext interface {
	Write(key []byte, value []byte) error
	Configuration() Configuration
}

type Reducer interface {
	Reduce(key string, values []string, ctx ReduceContext) error
}

type JobPlugin interface {
	// 如果自定义这个，后续别的程序使用此hash函数，可以定位key所在partition
	HashFunc() func([]byte) int64

	InputFormat() InputFormat
	Mapper() Mapper
	Reducer() Reducer
}
