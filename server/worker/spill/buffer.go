package spill

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
)

type MapOutputBuffer struct {
	dir           string
	numPartitions int
	memLimitBytes int

	// 真实 key/value bytes 连续写在这里。
	kvBuf []byte

	// meta 只记录每条 record 在 kvBuf 里的位置。
	meta []Meta

	spillSeq   int
	spillFiles []string

	hashFunc func([]byte) int64
}

func NewMapOutputBuffer(dir string, memLimitBytes int, numPartitions int, hashFun func([]byte) int64) (*MapOutputBuffer, error) {
	if memLimitBytes <= 0 {
		return nil, fmt.Errorf("memLimitBytes must be positive")
	}
	if numPartitions <= 0 {
		return nil, fmt.Errorf("numPartitions must be positive")
	}
	if hashFun == nil {
		hashFun = func(key []byte) int64 {
			return int64(hashPartition(key, numPartitions))
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	return &MapOutputBuffer{
		dir:           dir,
		memLimitBytes: memLimitBytes,
		numPartitions: numPartitions,
		kvBuf:         make([]byte, 0, memLimitBytes),
		meta:          make([]Meta, 0, 1024),
		spillFiles:    make([]string, 0),
		hashFunc:      hashFun,
	}, nil
}

// Emit 模拟 Hadoop Mapper 的 context.write(key, value)。
// 真实 key/value bytes 写入 kvBuf；meta 记录 offset/length/partition。
func (b *MapOutputBuffer) Emit(key, value []byte) error {
	partition := int(b.hashFunc(key) % int64(b.numPartitions))
	if partition < 0 {
		partition += b.numPartitions
	}

	// 直接写入key val到kvBuf，数据没有任何header
	keyOffset := len(b.kvBuf)
	b.kvBuf = append(b.kvBuf, key...)

	valOffset := len(b.kvBuf)
	b.kvBuf = append(b.kvBuf, value...)

	// 标记key val位置和长度，后续会排序meta
	b.meta = append(b.meta, Meta{
		Partition: partition,
		KeyOffset: keyOffset,
		KeyLen:    len(key),
		ValOffset: valOffset,
		ValLen:    len(value),
	})

	// 这里同时计算了meta占用的字节数和keyval占用的字节数
	if len(b.kvBuf)+len(b.meta)*metaMemoryBytes >= b.memLimitBytes {
		return b.spill()
	}

	return nil
}

// Finish 会把当前内存数据 spill，然后对所有 spill 文件做多路归并，生成最终 map.out 和 map.out.index。
func (b *MapOutputBuffer) Finish(outputPath string, indexPath string) ([]PartitionIndex, error) {
	if err := b.spill(); err != nil {
		return nil, err
	}

	index := make([]PartitionIndex, b.numPartitions)
	for i := range index {
		index[i].Offset = -1
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}
	defer outFile.Close()

	out := bufio.NewWriterSize(outFile, 4<<20)

	readers := make([]*spillReader, 0, len(b.spillFiles))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	h := &recordHeap{}
	heap.Init(h)

	seq := 0

	for _, path := range b.spillFiles {
		r, err := newSpillReader(path)
		if err != nil {
			return nil, err
		}

		readers = append(readers, r)
		readerID := len(readers) - 1

		rec, ok, err := r.Next()
		if err != nil {
			return nil, err
		}

		if ok {
			heap.Push(h, heapItem{
				rec:    rec,
				reader: readerID,
				seq:    seq,
			})
			seq++
		}
	}

	var offset int64

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		rec := item.rec

		if rec.Partition < 0 || rec.Partition >= b.numPartitions {
			return nil, fmt.Errorf("invalid partition: %d", rec.Partition)
		}

		p := rec.Partition

		if index[p].Offset == -1 {
			index[p].Offset = offset
		}

		n, err := writeRecord(out, rec.Key, rec.Value)
		if err != nil {
			return nil, err
		}

		offset += n
		index[p].Length += n

		next, ok, err := readers[item.reader].Next()
		if err != nil {
			return nil, err
		}

		if ok {
			heap.Push(h, heapItem{
				rec:    next,
				reader: item.reader,
				seq:    seq,
			})
			seq++
		}
	}

	if err := out.Flush(); err != nil {
		return nil, err
	}

	if err := writeIndex(indexPath, index); err != nil {
		return nil, err
	}

	return index, nil
}

func (b *MapOutputBuffer) Cleanup() error {
	var firstErr error

	for _, path := range b.spillFiles {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(indexPathForOutput(path)); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// spill 的关键点：
// 1. kvBuf 不动
// 2. 只排序 meta
// 3. 按排序后的 meta 顺序读取 kvBuf
// 4. 写成一个有序 spill 文件
func (b *MapOutputBuffer) spill() error {
	if len(b.meta) == 0 {
		return nil
	}

	compareMeta := func(a Meta, c Meta) int {
		if a.Partition < c.Partition {
			return -1
		}
		if a.Partition > c.Partition {
			return 1
		}

		ak := b.kvBuf[a.KeyOffset : a.KeyOffset+a.KeyLen]
		ck := b.kvBuf[c.KeyOffset : c.KeyOffset+c.KeyLen]

		return bytes.Compare(ak, ck)
	}

	sort.Slice(b.meta, func(i, j int) bool {
		return compareMeta(b.meta[i], b.meta[j]) < 0
	})

	path := filepath.Join(b.dir, fmt.Sprintf("spill-%06d.out", b.spillSeq))
	indexPath := indexPathForOutput(path)
	b.spillSeq++

	file, err := os.Create(path)
	if err != nil {
		return err
	}

	// bufio writer 这样写入数据的速度更快
	writer := bufio.NewWriterSize(file, 4<<20)

	index := make([]PartitionIndex, b.numPartitions)
	for i := range index {
		index[i].Offset = -1
	}
	var offset int64

	for _, m := range b.meta {
		key := b.kvBuf[m.KeyOffset : m.KeyOffset+m.KeyLen]
		val := b.kvBuf[m.ValOffset : m.ValOffset+m.ValLen]

		if index[m.Partition].Offset == -1 {
			index[m.Partition].Offset = offset
		}

		n, err := writeRecord(writer, key, val)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return err
		}
		offset += n
		index[m.Partition].Length += n
	}

	if err := writer.Flush(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}

	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}

	if err := writeIndex(indexPath, index); err != nil {
		_ = os.Remove(path)
		_ = os.Remove(indexPath)
		return err
	}

	b.spillFiles = append(b.spillFiles, path)

	// 清空当前内存 buffer，复用底层数组容量。
	b.kvBuf = b.kvBuf[:0]
	b.meta = b.meta[:0]

	return nil
}

func hashPartition(key []byte, numPartitions int) int {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(numPartitions))
}
