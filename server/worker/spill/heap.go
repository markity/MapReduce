package spill

import "bytes"

type heapItem struct {
	rec    DiskRecord
	reader int
	seq    int
}

type recordHeap []heapItem

func (h recordHeap) Len() int {
	return len(h)
}

func (h recordHeap) Less(i, j int) bool {
	cmp := compareDiskRecord(h[i].rec, h[j].rec)
	if cmp != 0 {
		return cmp < 0
	}

	// 完全相等时用 seq 保持稳定性。
	return h[i].seq < h[j].seq
}

func (h recordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *recordHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *recordHeap) Pop() any {
	old := *h
	n := len(old)

	item := old[n-1]
	*h = old[:n-1]

	return item
}

func compareDiskRecord(a, b DiskRecord) int {
	if a.Partition < b.Partition {
		return -1
	}
	if a.Partition > b.Partition {
		return 1
	}

	return bytes.Compare(a.Key, b.Key)
}
