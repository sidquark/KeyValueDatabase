package database

import (
	"sync"
	"time"

	"github.com/sidquark/KeyValueDatabase/internal/storage"
	"github.com/sidquark/KeyValueDatabase/internal/persistence"
)

// DB represents the main database instance
type DB struct {
	storage     *storage.HashTable
	log         *persistence.Log
	recovery    *persistence.Recovery
	config      *Config
	mutex       sync.RWMutex
	isClosed    bool
	closeChan   chan struct{}
}

// Config holds database configuration options
type Config struct {
	NumBuckets          int
	LogPath             string
	CompactionInterval  time.Duration
	PersistenceInterval time.Duration
	AutoRecover         bool
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		NumBuckets:          1024,
		LogPath:             "./data",
		CompactionInterval:  10 * time.Minute,
		PersistenceInterval: 5 * time.Second,
		AutoRecover:         true,
	}
}

// New creates a new database instance
func New(config *Config) (*DB, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Create storage
	store := storage.NewHashTable(config.NumBuckets)
	
	// Create recovery instance
	recovery := persistence.NewRecovery(config.LogPath)
	
	// Create log
	log, err := persistence.NewLog(config.LogPath)
	if err != nil {
		return nil, NewDatabaseError("initialization", "", err)
	}

	db := &DB{
		storage:   store,
		log:       log,
		recovery:  recovery,
		config:    config,
		closeChan: make(chan struct{}),
	}

	// Recover from log if enabled
	if config.AutoRecover {
		err = db.recoverFromLog()
		if err != nil {
			log.Close()
			return nil, NewDatabaseError("recovery", "", err)
		}
	}

	// Start background tasks
	go db.startBackgroundTasks()

	return db, nil
}

// recoverFromLog applies all operations from the log
func (db *DB) recoverFromLog() error {
	entries, err := db.recovery.RecoverEntries()
	if err != nil {
		return err
	}
	
	// Replay log entries
	for _, entry := range entries {
		switch entry.Operation {
		case persistence.OperationSet:
			db.storage.Set(entry.Key, entry.Value)
		case persistence.OperationDelete:
			db.storage.Delete(entry.Key)
		}
	}
	
	return nil
}

// startBackgroundTasks starts all background tasks
func (db *DB) startBackgroundTasks() {
	// Start log compaction
	compactionTicker := time.NewTicker(db.config.CompactionInterval)
	defer compactionTicker.Stop()
	
	for {
		select {
		case <-compactionTicker.C:
			// Compact log
			db.log.Compact()
		case <-db.closeChan:
			return
		}
	}
}

// Close closes the database
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	
	if db.isClosed {
		return nil
	}
	
	// Signal background tasks to stop
	close(db.closeChan)
	
	// Close log
	err := db.log.Close()
	if err != nil {
		return NewDatabaseError("close", "", err)
	}
	
	db.isClosed = true
	
	return nil
}
