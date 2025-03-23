package persistence

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Recovery handles the database recovery from the log
type Recovery struct {
	logDir string
}

// NewRecovery creates a new recovery instance
func NewRecovery(logDir string) *Recovery {
	return &Recovery{
		logDir: logDir,
	}
}

// RecoverEntries reads the log and returns all valid entries
func (r *Recovery) RecoverEntries() ([]*LogEntry, error) {
	logPath := filepath.Join(r.logDir, "database.log")
	
	// Check if log file exists
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		// No log file, nothing to recover
		return nil, nil
	}
	
	file, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file for recovery: %w", err)
	}
	defer file.Close()
	
	reader := bufio.NewReader(file)
	
	var entries []*LogEntry
	var offset int64 = 0
	
	for {
		entry, bytesRead, err := r.readEntry(reader)
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			// Skip corrupted entry and continue
			fmt.Printf("Warning: Skipping corrupted entry at offset %d: %v\n", offset, err)
			offset += bytesRead
			continue
		}
		
		offset += bytesRead
		entries = append(entries, entry)
	}
	
	return entries, nil
}

// readEntry reads a single entry from the log
func (r *Recovery) readEntry(reader *bufio.Reader) (*LogEntry, int64, error) {
	var bytesRead int64 = 0
	
	// Read timestamp (8 bytes)
	timeBytes := make([]byte, 8)
	n, err := io.ReadFull(reader, timeBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	timestamp := int64(binary.LittleEndian.Uint64(timeBytes))
	
	// Read operation (1 byte)
	opByte := make([]byte, 1)
	n, err = io.ReadFull(reader, opByte)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	operation := LogOperation(opByte[0])
	
	// Read key length (2 bytes)
	keyLenBytes := make([]byte, 2)
	n, err = io.ReadFull(reader, keyLenBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	keyLen := binary.LittleEndian.Uint16(keyLenBytes)
	
	// Read key
	keyBytes := make([]byte, keyLen)
	n, err = io.ReadFull(reader, keyBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	key := string(keyBytes)
	
	// Read value length (4 bytes)
	valueLenBytes := make([]byte, 4)
	n, err = io.ReadFull(reader, valueLenBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	valueLen := binary.LittleEndian.Uint32(valueLenBytes)
	
	// Read value (if present)
	var value []byte
	if valueLen > 0 {
		value = make([]byte, valueLen)
		n, err = io.ReadFull(reader, value)
		bytesRead += int64(n)
		if err != nil {
			return nil, bytesRead, err
		}
	}
	
	// Read checksum (4 bytes)
	checksumBytes := make([]byte, 4)
	n, err = io.ReadFull(reader, checksumBytes)
	bytesRead += int64(n)
	if err != nil {
		return nil, bytesRead, err
	}
	checksum := binary.LittleEndian.Uint32(checksumBytes)
	
	// Create log entry
	entry := &LogEntry{
		Timestamp: timestamp,
		Operation: operation,
		Key:       key,
		Value:     value,
		Checksum:  checksum,
	}
	
	// Validate checksum
	var data []byte
	
	// Add timestamp
	timeBytes = make([]byte, 8)
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
	
	calculatedChecksum := crc32.ChecksumIEEE(data)
	if calculatedChecksum != checksum {
		return nil, bytesRead, fmt.Errorf("checksum mismatch: expected %d, got %d", checksum, calculatedChecksum)
	}
	
	return entry, bytesRead, nil
}
