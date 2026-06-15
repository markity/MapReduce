package spill

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// record 落盘格式：
//
// uint32 keyLen
// uint32 valueLen
// key bytes
// value bytes
func writeRecord(w io.Writer, key []byte, value []byte) (int64, error) {
	if len(key) > math.MaxUint32 {
		return 0, fmt.Errorf("key length exceeds uint32: %d", len(key))
	}
	if len(value) > math.MaxUint32 {
		return 0, fmt.Errorf("value length exceeds uint32: %d", len(value))
	}

	var header [recordHeaderBytes]byte

	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))

	var written int64

	n, err := writeAll(w, header[:])
	written += int64(n)
	if err != nil {
		return written, err
	}

	n, err = writeAll(w, key)
	written += int64(n)
	if err != nil {
		return written, err
	}

	n, err = writeAll(w, value)
	written += int64(n)
	if err != nil {
		return written, err
	}

	return written, nil
}

func readRecord(r io.Reader) (DiskRecord, error) {
	var header [recordHeaderBytes]byte

	_, err := io.ReadFull(r, header[:])
	if err != nil {
		return DiskRecord{}, err
	}

	keyLen := int(binary.LittleEndian.Uint32(header[0:4]))
	valLen := int(binary.LittleEndian.Uint32(header[4:8]))

	key := make([]byte, keyLen)
	value := make([]byte, valLen)

	if _, err := io.ReadFull(r, key); err != nil {
		return DiskRecord{}, err
	}

	if _, err := io.ReadFull(r, value); err != nil {
		return DiskRecord{}, err
	}

	return DiskRecord{
		Key:   key,
		Value: value,
	}, nil
}

func ReadRecord(r io.Reader) (DiskRecord, error) {
	return readRecord(r)
}

func writeAll(w io.Writer, p []byte) (int, error) {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, err
}
