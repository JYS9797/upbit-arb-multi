package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type JSONLWriter struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

func NewJSONLWriter(outDir, filename string) (*JSONLWriter, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil { return nil, err }
	path := filepath.Join(outDir, filename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil { return nil, err }
	return &JSONLWriter{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

func (j *JSONLWriter) Write(v any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	b, err := json.Marshal(v)
	if err != nil { return err }
	if _, err := j.w.Write(b); err != nil { return err }
	if err := j.w.WriteByte('\n'); err != nil { return err }
	return j.w.Flush()
}

func (j *JSONLWriter) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.w != nil { _ = j.w.Flush() }
	if j.f != nil { return j.f.Close() }
	return nil
}
