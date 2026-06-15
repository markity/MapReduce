package spill

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func dumpOutput(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	r := bufio.NewReader(file)

	for {
		rec, err := readRecord(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		fmt.Printf("partition=%d key=%s value=%s\n", rec.Partition, rec.Key, rec.Value)
	}
}
