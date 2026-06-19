package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func NewWal(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %v", err)
	}
	return &WAL{file: file}, nil
}

func (w *WAL) AppendSet(key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := fmt.Sprintf("SET,%s,%s\n", key, value)
	if _, err := w.file.WriteString(entry); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) AppendDelete(key string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := fmt.Sprintf("DEL,%s\n", key)
	if _, err := w.file.WriteString(entry); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func Replay(filename string, store *Store) error {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, ",", 3)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "SET":
			if len(parts) == 3 {
				store.Set(parts[1], parts[2])
			}
		case "DEL":
			if len(parts) >= 2 {
				store.Delete(parts[1])
			}
		}
	}
	return scanner.Err()
}
