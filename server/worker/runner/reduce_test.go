package runner

import (
	"errors"
	"reflect"
	"testing"

	mrplugin "mapreduce/plugin"
)

func TestReduceContextCommitsOutput(t *testing.T) {
	rec := &outputCommitRecorder{}
	writer := &recordingRecordWriter{rec: rec}
	format := &recordingOutputFormat{
		rec:    rec,
		writer: writer,
	}

	ctx, err := newReduceContext("/tmp/reduce-output", mrplugin.NewConfiguration(), format)
	if err != nil {
		t.Fatalf("newReduceContext failed: %v", err)
	}
	if err := ctx.Write([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := ctx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	want := []string{"setup", "create-writer", "write:k=v", "close-writer", "commit"}
	if !reflect.DeepEqual(rec.events, want) {
		t.Fatalf("events = %v, want %v", rec.events, want)
	}
}

func TestReduceContextAbortsOutput(t *testing.T) {
	rec := &outputCommitRecorder{}
	format := &recordingOutputFormat{
		rec:    rec,
		writer: &recordingRecordWriter{rec: rec},
	}

	ctx, err := newReduceContext("/tmp/reduce-output", mrplugin.NewConfiguration(), format)
	if err != nil {
		t.Fatalf("newReduceContext failed: %v", err)
	}
	if err := ctx.Abort(); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	want := []string{"setup", "create-writer", "close-writer", "abort"}
	if !reflect.DeepEqual(rec.events, want) {
		t.Fatalf("events = %v, want %v", rec.events, want)
	}
}

func TestReduceContextAbortsWhenWriterCreateFails(t *testing.T) {
	rec := &outputCommitRecorder{}
	format := &recordingOutputFormat{
		rec:       rec,
		createErr: errors.New("create failed"),
	}

	_, err := newReduceContext("/tmp/reduce-output", mrplugin.NewConfiguration(), format)
	if err == nil {
		t.Fatalf("newReduceContext succeeded, want error")
	}

	want := []string{"setup", "create-writer", "abort"}
	if !reflect.DeepEqual(rec.events, want) {
		t.Fatalf("events = %v, want %v", rec.events, want)
	}
}

type outputCommitRecorder struct {
	events []string
}

type recordingOutputFormat struct {
	rec       *outputCommitRecorder
	writer    mrplugin.RecordWriter
	createErr error
}

func (f *recordingOutputFormat) CreateRecordWriter(outputPath string, conf mrplugin.Configuration) (mrplugin.RecordWriter, error) {
	f.rec.events = append(f.rec.events, "create-writer")
	if f.createErr != nil {
		return nil, f.createErr
	}
	return f.writer, nil
}

func (f *recordingOutputFormat) OutputCommitter() mrplugin.OutputCommitter {
	return &recordingOutputCommitter{rec: f.rec}
}

type recordingRecordWriter struct {
	rec *outputCommitRecorder
}

func (w *recordingRecordWriter) Write(key []byte, value []byte) error {
	w.rec.events = append(w.rec.events, "write:"+string(key)+"="+string(value))
	return nil
}

func (w *recordingRecordWriter) Close() error {
	w.rec.events = append(w.rec.events, "close-writer")
	return nil
}

type recordingOutputCommitter struct {
	rec *outputCommitRecorder
}

func (c *recordingOutputCommitter) SetupTask(outputPath string, conf mrplugin.Configuration) error {
	c.rec.events = append(c.rec.events, "setup")
	return nil
}

func (c *recordingOutputCommitter) CommitTask(outputPath string, conf mrplugin.Configuration) error {
	c.rec.events = append(c.rec.events, "commit")
	return nil
}

func (c *recordingOutputCommitter) AbortTask(outputPath string, conf mrplugin.Configuration) error {
	c.rec.events = append(c.rec.events, "abort")
	return nil
}
