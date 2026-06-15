package scheduler

import (
	"mapreduce/server/worker/entity"
)

type reportQueue struct {
	reports map[entity.TaskAttemptKey]entity.TaskReport
}

func newReportQueue() *reportQueue {
	return &reportQueue{reports: make(map[entity.TaskAttemptKey]entity.TaskReport)}
}

func (q *reportQueue) Add(report entity.TaskReport) {
	q.reports[report.Key()] = report
}

func (q *reportQueue) Snapshot() []entity.TaskReport {
	out := make([]entity.TaskReport, 0, len(q.reports))
	for _, report := range q.reports {
		out = append(out, report)
	}
	return out
}

func (q *reportQueue) Ack(keys []entity.TaskAttemptKey) {
	for _, key := range keys {
		delete(q.reports, key)
	}
}
