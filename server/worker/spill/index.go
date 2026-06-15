package spill

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func indexPathForOutput(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return path + ".index"
	}
	return path[:len(path)-len(ext)] + ".index"
}

// index 文件格式：
//
// int64 partition0Offset
// int64 partition0Length
// int64 partition1Offset
// int64 partition1Length
// ...
func writeIndex(path string, index []PartitionIndex) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	for _, item := range index {
		if err := binary.Write(writer, binary.LittleEndian, item.Offset); err != nil {
			return err
		}

		if err := binary.Write(writer, binary.LittleEndian, item.Length); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func WriteIndex(path string, index []PartitionIndex) error {
	return writeIndex(path, index)
}

func readIndex(path string, numPartitions int) ([]PartitionIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if numPartitions == 0 {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if info.Size()%16 != 0 {
			return nil, fmt.Errorf("invalid index size: %d", info.Size())
		}
		numPartitions = int(info.Size() / 16)
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}

	index := make([]PartitionIndex, numPartitions)

	for i := 0; i < numPartitions; i++ {
		if err := binary.Read(file, binary.LittleEndian, &index[i].Offset); err != nil {
			return nil, err
		}

		if err := binary.Read(file, binary.LittleEndian, &index[i].Length); err != nil {
			return nil, err
		}
	}

	return index, nil
}

func ReadIndex(path string, numPartitions int) ([]PartitionIndex, error) {
	return readIndex(path, numPartitions)
}
