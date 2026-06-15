package spill

const (
	recordHeaderBytes = 8
	metaMemoryBytes   = 40 // 粗略估算：Meta 结构体在内存里的占用
)

type Meta struct {
	Partition int

	KeyOffset int
	KeyLen    int

	ValOffset int
	ValLen    int
}

type DiskRecord struct {
	Partition int
	Key       []byte
	Value     []byte
}

type PartitionIndex struct {
	Offset int64
	Length int64
}
