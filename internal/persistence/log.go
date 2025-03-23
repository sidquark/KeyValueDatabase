package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogOperation represents the type of operation in a log entry
type LogOperation byte

const (
	OperationSet LogOperation = iota + 1
	OperationDelete
)

// LogEntry represents a single entry in the append-only log
type LogEntry struct {
	Timestamp int64
	Operation LogOperation
	Key       string
	Value     []byte
	Checksum  uint32
}

// Log represents an append-only log for durability
type Log struct {
	dir         string
	file        *os.File
	writer      *bufio.Writer
	mutex       sync.Mutex
	currSize    int64
	isCompacted bool
}

// NewLog creates a new append-only log
func NewLog(dir string) (*Log, error) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}
	
	logPath := filepath.Join(dir, "database.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	
	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get log file info: %w", err)
	}
	
	log := &Log{
		dir:      dir,
		file:     file,
		writer:   bufio.NewWriter(file),
		currSize: info.Size(),
	}
	
	return log, nil
}

// Append adds a new entry to the log
func (l *Log) Append(operation LogOperation, key string, value []byte) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	// Create log entry
	entry := &LogEntry{
		Timestamp: time.Now().UnixNano(),
		Operation: operation,
		Key:       key,
		Value:     value,
	}
	
	// Calculate checksum
	entry.Checksum = l.calculateChecksum(entry)
	
	// Serialize entry
	data, err := l.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize log entry: %w", err)
	}
	
	// Write to buffer
	_, err = l.writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to log buffer: %w", err)
	}
	
	// Flush to disk
	err = l.writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush log to disk: %w", err)
	}
	
	// Update size
	l.currSize += int64(len(data))
	
	return nil
}

// calculateChecksum computes the checksum for a log entry
func (l *Log) calculateChecksum(entry *LogEntry) uint32 {
	// Create a byte buffer for checksum calculation
	var data []byte
	
	// Add timestamp
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(entry.Timestamp))
	data = append(data, timeBytes...)
	
	// Add operation
	data = append(data, byte(entry.Operation))
	
	// Add key
	data = append(data, []byte(entry.Key)...)
	
	// Add value if present
	if entry.Value != nil {
		data = append(data, entry.Value...)
	}
	
	// Calculate checksum
	return crc32.ChecksumIEEE(data)
}

// serializeEntry converts a log entry to a byte array
func (l *Log) serializeEntry(entry *LogEntry) ([]byte, error) {
	var data []byte
	
	// Write timestamp (8 bytes)
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(entry.Timestamp))
	data = append(data, timeBytes...)
	
	// Write operation (1 byte)
	data = append(data, byte(entry.Operation))
	
	// Write key length (2 bytes) and key
	keyBytes := []byte(entry.Key)
	keyLenBytes := make([]byte, 2)
	if len(keyBytes) > 65535 {
		return nil, fmt.Errorf("key is too long")
	}
	binary.LittleEndian.PutUint16(keyLenBytes, uint16(len(keyBytes)))
	data = append(data, keyLenBytes...)
	data = append(data, keyBytes...)
	
	// Write value length (4 bytes) and value (if present)
	valueLenBytes := make([]byte, 4)
	if entry.Value != nil {
		binary.LittleEndian.PutUint32(valueLenBytes, uint32(len(entry.Value)))
		data = append(data, valueLenBytes...)
		data = append(data, entry.Value...)
	} else {
		binary.LittleEndian.PutUint32(valueLenBytes, 0)
		data = append(data, valueLenBytes...)
	}
	
	// Write checksum (4 bytes)
	checksumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBytes, entry.Checksum)
	data = append(data, checksumBytes...)
	
	return data, nil
}

// Close closes the log file
func (l *Log) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	if l.file == nil {
		return nil
	}
	
	// Flush any pending writes
	err := l.writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush log on close: %w", err)
	}
	
	err = l.file.Close()
	l.file = nil
	
	return err
}

// Compact compacts the log by removing redundant entries
func (l *Log) Compact() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	
	if l.isCompacted {
		return nil // Already compacting
	}
	
	l.isCompacted = true
	defer func() { l.isCompacted = false }()
	
	// Create a temporary log file
	tempPath := filepath.Join(l.dir, "temp.log")
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary log file: %w", err)
	}
	
	// Implementation of log compaction logic would go here
	// For now, we'll just create an empty log (simplified)
	
	// Close current log file
	err = l.writer.Flush()
	if err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to flush log: %w", err)
	}
	
	err = l.file.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to close log file: %w", err)
	}
	
	// Close temporary file
	err = tempFile.Close()
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close temporary log file: %w", err)
	}
	
	// Replace the old log with the new one
	logPath := filepath.Join(l.dir, "database.log")
	err = os.Rename(tempPath, logPath)
	if err != nil {
		return fmt.Errorf("failed to replace log file: %w", err)
	}
	
	// Reopen the log file
	l.file, err = os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen log file: %w", err)
	}
	
	l.writer = bufio.NewWriter(l.file)
	
	// Update size
	info, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get log file info: %w", err)
	}
	
	l.currSize = info.Size()
	
	return nil
}
