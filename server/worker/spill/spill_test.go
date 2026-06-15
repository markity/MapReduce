package spill

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRecordRoundTripPreservesBinaryBytes(t *testing.T) {
	key := []byte{'k', '\t', 'e', '\n', 'y', 0}
	value := []byte{'v', '\t', 'a', '\n', 'l', 0xff}

	var buf bytes.Buffer
	written, err := writeRecord(&buf, key, value)
	if err != nil {
		t.Fatalf("writeRecord failed: %v", err)
	}
	if want := int64(recordHeaderBytes + len(key) + len(value)); written != want {
		t.Fatalf("written bytes = %d, want %d", written, want)
	}

	rec, err := readRecord(&buf)
	if err != nil {
		t.Fatalf("readRecord failed: %v", err)
	}
	if !bytes.Equal(rec.Key, key) {
		t.Fatalf("key = %q, want %q", rec.Key, key)
	}
	if !bytes.Equal(rec.Value, value) {
		t.Fatalf("value = %q, want %q", rec.Value, value)
	}
	if _, err := readRecord(&buf); err != io.EOF {
		t.Fatalf("second read error = %v, want EOF", err)
	}
}

func TestFinishMergesSpillsAndWritesPartitionIndex(t *testing.T) {
	tmp := t.TempDir()
	buf, err := NewMapOutputBuffer(filepath.Join(tmp, "spill"), 64, 4, nil)
	if err != nil {
		t.Fatalf("NewMapOutputBuffer failed: %v", err)
	}
	defer func() {
		if err := buf.Cleanup(); err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}
	}()

	inputs := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("delta"), []byte("4")},
		{[]byte("alpha"), []byte("1")},
		{[]byte("echo"), []byte("5")},
		{[]byte("bravo"), []byte("2")},
		{[]byte("charlie"), []byte("3")},
		{[]byte("foxtrot"), []byte("6")},
		{[]byte("golf"), []byte("7")},
	}
	for _, in := range inputs {
		if err := buf.Emit(in.key, in.value); err != nil {
			t.Fatalf("Emit(%q, %q) failed: %v", in.key, in.value, err)
		}
	}
	if len(buf.spillFiles) < 2 {
		t.Fatalf("expected small memory limit to create multiple spill files, got %d", len(buf.spillFiles))
	}

	outputPath := filepath.Join(tmp, "map.out")
	indexPath := filepath.Join(tmp, "map.out.index")
	index, err := buf.Finish(outputPath, indexPath)
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	indexFromDisk, err := readIndex(indexPath, 4)
	if err != nil {
		t.Fatalf("readIndex failed: %v", err)
	}
	if !reflect.DeepEqual(indexFromDisk, index) {
		t.Fatalf("index from disk = %+v, want %+v", indexFromDisk, index)
	}

	records, offsets, lengths := readAllRecordsWithOffsets(t, outputPath, index)
	if len(records) != len(inputs) {
		t.Fatalf("record count = %d, want %d", len(records), len(inputs))
	}
	for i := 1; i < len(records); i++ {
		if compareDiskRecord(records[i-1], records[i]) > 0 {
			t.Fatalf("records are not sorted at %d: %+v > %+v", i, records[i-1], records[i])
		}
	}

	expectedByPartition := make([]PartitionIndex, 4)
	for i := range expectedByPartition {
		expectedByPartition[i].Offset = -1
	}
	for i, rec := range records {
		item := &expectedByPartition[rec.Partition]
		if item.Offset == -1 {
			item.Offset = offsets[i]
		}
		item.Length += lengths[i]
	}
	if !reflect.DeepEqual(index, expectedByPartition) {
		t.Fatalf("index = %+v, want %+v", index, expectedByPartition)
	}

	wantCounts := make(map[string]int)
	for _, in := range inputs {
		partition := hashPartition(in.key, 4)
		wantCounts[string(recordIdentity(partition, in.key, in.value))]++
	}
	gotCounts := make(map[string]int)
	for _, rec := range records {
		gotCounts[string(recordIdentity(rec.Partition, rec.Key, rec.Value))]++
	}
	if !reflect.DeepEqual(gotCounts, wantCounts) {
		t.Fatalf("records = %+v, want %+v", gotCounts, wantCounts)
	}

	for _, path := range buf.spillFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected spill file before cleanup: %v", err)
		}
		if _, err := os.Stat(indexPathForOutput(path)); err != nil {
			t.Fatalf("expected spill index before cleanup: %v", err)
		}
	}
	if err := buf.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	if err := buf.Cleanup(); err != nil {
		t.Fatalf("second Cleanup failed: %v", err)
	}
	for _, path := range buf.spillFiles {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("spill file still exists or unexpected stat error: %v", err)
		}
		if _, err := os.Stat(indexPathForOutput(path)); !os.IsNotExist(err) {
			t.Fatalf("spill index still exists or unexpected stat error: %v", err)
		}
	}
}

func TestEmptyFinishWritesEmptyOutputAndEmptyIndex(t *testing.T) {
	tmp := t.TempDir()
	buf, err := NewMapOutputBuffer(filepath.Join(tmp, "spill"), 128, 3, nil)
	if err != nil {
		t.Fatalf("NewMapOutputBuffer failed: %v", err)
	}

	outputPath := filepath.Join(tmp, "map.out")
	indexPath := filepath.Join(tmp, "map.out.index")
	index, err := buf.Finish(outputPath, indexPath)
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	want := []PartitionIndex{
		{Offset: -1},
		{Offset: -1},
		{Offset: -1},
	}
	if !reflect.DeepEqual(index, want) {
		t.Fatalf("index = %+v, want %+v", index, want)
	}

	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat output failed: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("output size = %d, want 0", info.Size())
	}

	indexFromDisk, err := readIndex(indexPath, 3)
	if err != nil {
		t.Fatalf("readIndex failed: %v", err)
	}
	if !reflect.DeepEqual(indexFromDisk, want) {
		t.Fatalf("index from disk = %+v, want %+v", indexFromDisk, want)
	}
}

func TestEmitUsesCustomPartitioner(t *testing.T) {
	tmp := t.TempDir()
	buf, err := NewMapOutputBuffer(filepath.Join(tmp, "spill"), 128, 3, fixedPartitioner{partition: 2})
	if err != nil {
		t.Fatalf("NewMapOutputBuffer failed: %v", err)
	}
	if err := buf.Emit([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Emit failed: %v", err)
	}
	if got := buf.meta[0].Partition; got != 2 {
		t.Fatalf("partition = %d, want 2", got)
	}
}

func TestEmitRejectsInvalidPartitioner(t *testing.T) {
	tmp := t.TempDir()
	buf, err := NewMapOutputBuffer(filepath.Join(tmp, "spill"), 128, 3, fixedPartitioner{partition: -1})
	if err != nil {
		t.Fatalf("NewMapOutputBuffer failed: %v", err)
	}
	if err := buf.Emit([]byte("key"), []byte("value")); err != nil {
		if !strings.Contains(err.Error(), "invalid partition") {
			t.Fatalf("Emit error = %v, want invalid partition", err)
		}
		return
	}
	t.Fatalf("Emit succeeded, want invalid partition error")
}

type fixedPartitioner struct {
	partition int
}

func (p fixedPartitioner) Partition(key []byte, value []byte, numPartitions int) int {
	return p.partition
}

func TestMergeReaderMergesMultipleOutputIndexPairs(t *testing.T) {
	tmp := t.TempDir()
	firstPath := filepath.Join(tmp, "first.out")
	firstIndex := indexPathForOutput(firstPath)
	secondPath := filepath.Join(tmp, "second.out")
	secondIndex := indexPathForOutput(secondPath)

	firstIndexItems := writePartitionedOutput(t, firstPath, 3, []DiskRecord{
		{Partition: 0, Key: []byte("apple"), Value: []byte("1")},
		{Partition: 1, Key: []byte("banana"), Value: []byte("2")},
		{Partition: 2, Key: []byte("carrot"), Value: []byte("3")},
	})
	if err := writeIndex(firstIndex, firstIndexItems); err != nil {
		t.Fatalf("write first index failed: %v", err)
	}

	secondIndexItems := writePartitionedOutput(t, secondPath, 3, []DiskRecord{
		{Partition: 0, Key: []byte("apricot"), Value: []byte("4")},
		{Partition: 1, Key: []byte("avocado"), Value: []byte("5")},
		{Partition: 2, Key: []byte("blueberry"), Value: []byte("6")},
	})
	if err := writeIndex(secondIndex, secondIndexItems); err != nil {
		t.Fatalf("write second index failed: %v", err)
	}

	reader, err := NewMergeReader([]string{firstPath, secondPath})
	if err != nil {
		t.Fatalf("NewMergeReader failed: %v", err)
	}
	defer reader.Close()

	var got []DiskRecord
	for {
		ok, err := reader.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, cloneRecord(reader.Record()))
	}

	want := []DiskRecord{
		{Partition: 0, Key: []byte("apple"), Value: []byte("1")},
		{Partition: 0, Key: []byte("apricot"), Value: []byte("4")},
		{Partition: 1, Key: []byte("avocado"), Value: []byte("5")},
		{Partition: 1, Key: []byte("banana"), Value: []byte("2")},
		{Partition: 2, Key: []byte("blueberry"), Value: []byte("6")},
		{Partition: 2, Key: []byte("carrot"), Value: []byte("3")},
	}
	if !recordsEqual(got, want) {
		t.Fatalf("records = %+v, want %+v", got, want)
	}
}

func readAllRecordsWithOffsets(t *testing.T, path string, index []PartitionIndex) ([]DiskRecord, []int64, []int64) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open output failed: %v", err)
	}
	defer file.Close()

	var records []DiskRecord
	var offsets []int64
	var lengths []int64
	for partition, item := range index {
		if item.Length == 0 {
			continue
		}
		if _, err := file.Seek(item.Offset, io.SeekStart); err != nil {
			t.Fatalf("seek partition %d failed: %v", partition, err)
		}
		reader := bufio.NewReader(file)
		read := int64(0)
		for read < item.Length {
			rec, err := readRecord(reader)
			if err != nil {
				t.Fatalf("readRecord failed: %v", err)
			}
			length := int64(recordHeaderBytes + len(rec.Key) + len(rec.Value))
			if read+length > item.Length {
				t.Fatalf("record crosses partition boundary")
			}
			rec.Partition = partition
			records = append(records, rec)
			offsets = append(offsets, item.Offset+read)
			lengths = append(lengths, length)
			read += length
		}
	}
	return records, offsets, lengths
}

func writePartitionedOutput(t *testing.T, path string, numPartitions int, records []DiskRecord) []PartitionIndex {
	t.Helper()

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create output failed: %v", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	index := make([]PartitionIndex, numPartitions)
	for i := range index {
		index[i].Offset = -1
	}
	var offset int64
	for _, rec := range records {
		if index[rec.Partition].Offset == -1 {
			index[rec.Partition].Offset = offset
		}
		n, err := writeRecord(writer, rec.Key, rec.Value)
		if err != nil {
			t.Fatalf("writeRecord failed: %v", err)
		}
		index[rec.Partition].Length += n
		offset += n
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush output failed: %v", err)
	}
	return index
}

func recordIdentity(partition int, key []byte, value []byte) []byte {
	var out bytes.Buffer
	out.WriteByte(byte(partition))
	out.WriteByte(0)
	out.Write(key)
	out.WriteByte(0)
	out.Write(value)
	return out.Bytes()
}

func cloneRecord(rec DiskRecord) DiskRecord {
	return DiskRecord{
		Partition: rec.Partition,
		Key:       append([]byte(nil), rec.Key...),
		Value:     append([]byte(nil), rec.Value...),
	}
}

func recordsEqual(a []DiskRecord, b []DiskRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Partition != b[i].Partition {
			return false
		}
		if !bytes.Equal(a[i].Key, b[i].Key) {
			return false
		}
		if !bytes.Equal(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}
