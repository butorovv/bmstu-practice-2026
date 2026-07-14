package generator

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type eventLog struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	buffer  *bufio.Writer
	encoder *json.Encoder
	err     error
}

func newEventLog(resultPath string) (*eventLog, error) {
	directory := filepath.Dir(resultPath)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create result directory: %w", err)
	}

	extension := filepath.Ext(resultPath)
	path := strings.TrimSuffix(resultPath, extension) + ".jsonl"
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create event log: %w", err)
	}
	buffer := bufio.NewWriterSize(file, 64*1024)
	return &eventLog{
		path:    path,
		file:    file,
		buffer:  buffer,
		encoder: json.NewEncoder(buffer),
	}, nil
}

func (l *eventLog) Write(record acceptedBatchRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	l.err = l.encoder.Encode(record)
	return l.err
}

func (l *eventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	flushErr := l.buffer.Flush()
	closeErr := l.file.Close()
	return errors.Join(l.err, flushErr, closeErr)
}

func writeResult(path string, result Result) error {
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	encoded = append(encoded, '\n')

	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, encoded, 0o644); err != nil {
		return fmt.Errorf("write temporary result: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace result: %w", err)
	}
	return nil
}
