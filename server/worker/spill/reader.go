package spill

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
)

type spillReader struct {
	file      *os.File
	r         *bufio.Reader
	index     []PartitionIndex
	partition int
	remaining int64
}

func newSpillReader(path string) (*spillReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	index, err := readIndex(indexPathForOutput(path), 0)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &spillReader{
		file:      file,
		r:         bufio.NewReaderSize(file, 4<<20),
		index:     index,
		partition: -1,
	}, nil
}

func (r *spillReader) Next() (DiskRecord, bool, error) {
	for r.remaining == 0 {
		r.partition++
		if r.partition >= len(r.index) {
			return DiskRecord{}, false, nil
		}
		item := r.index[r.partition]
		if item.Length == 0 {
			continue
		}
		if _, err := r.file.Seek(item.Offset, io.SeekStart); err != nil {
			return DiskRecord{}, false, err
		}
		r.r.Reset(r.file)
		r.remaining = item.Length
	}

	rec, err := readRecord(r.r)
	if err == io.EOF {
		return DiskRecord{}, false, nil
	}
	if err != nil {
		return DiskRecord{}, false, err
	}
	n := int64(recordHeaderBytes + len(rec.Key) + len(rec.Value))
	if n > r.remaining {
		return DiskRecord{}, false, fmt.Errorf("record exceeds partition range")
	}
	r.remaining -= n
	rec.Partition = r.partition

	return rec, true, nil
}

func (r *spillReader) Close() error {
	return r.file.Close()
}

type MergeReader struct {
	readers []*spillReader
	heap    recordHeap
	seq     int
	current DiskRecord
}

func NewMergeReader(paths []string) (*MergeReader, error) {
	m := &MergeReader{
		readers: make([]*spillReader, 0, len(paths)),
	}
	heap.Init(&m.heap)

	for _, path := range paths {
		reader, err := newSpillReader(path)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.readers = append(m.readers, reader)
		readerID := len(m.readers) - 1

		rec, ok, err := reader.Next()
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		if ok {
			heap.Push(&m.heap, heapItem{
				rec:    rec,
				reader: readerID,
				seq:    m.seq,
			})
			m.seq++
		}
	}

	return m, nil
}

func (m *MergeReader) Next() (bool, error) {
	if m.heap.Len() == 0 {
		return false, nil
	}

	item := heap.Pop(&m.heap).(heapItem)
	m.current = item.rec

	next, ok, err := m.readers[item.reader].Next()
	if err != nil {
		return false, err
	}
	if ok {
		heap.Push(&m.heap, heapItem{
			rec:    next,
			reader: item.reader,
			seq:    m.seq,
		})
		m.seq++
	}

	return true, nil
}

func (m *MergeReader) Record() DiskRecord {
	return m.current
}

func (m *MergeReader) Close() error {
	var firstErr error
	for _, reader := range m.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
