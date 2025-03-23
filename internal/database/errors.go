package database

import (
	"errors"
	"fmt"
)

// Common database errors
var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrEmptyKey        = errors.New("key cannot be empty")
	ErrInvalidKeyType  = errors.New("key must be a string")
	ErrNilValue        = errors.New("value cannot be nil")
	ErrDatabaseClosed  = errors.New("database is closed")
	ErrLogWriteFailed  = errors.New("failed to write to log")
	ErrCorruptedEntry  = errors.New("log entry is corrupted")
	ErrRecoveryFailed  = errors.New("failed to recover from log")
)

// DatabaseError wraps database-specific errors with context
type DatabaseError struct {
	Operation string
	Key       string
	Err       error
}

// Error implements the error interface
func (e *DatabaseError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s operation failed for key '%s': %v", e.Operation, e.Key, e.Err)
	}
	return fmt.Sprintf("%s operation failed: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error
func (e *DatabaseError) Unwrap() error {
	return e.Err
}

// NewDatabaseError creates a new database error
func NewDatabaseError(operation, key string, err error) *DatabaseError {
	return &DatabaseError{
		Operation: operation,
		Key:       key,
		Err:       err,
	}
}
