package database

import (
	"github.com/sidquark/KeyValueDatabase/internal/persistence"
)

// Set stores a value for a given key
func (db *DB) Set(key string, value []byte) error {
	// Check if database is closed
	db.mutex.RLock()
	if db.isClosed {
		db.mutex.RUnlock()
		return ErrDatabaseClosed
	}
	db.mutex.RUnlock()
	
	// Input validation
	if key == "" {
		return ErrEmptyKey
	}
	
	if value == nil {
		return ErrNilValue
	}

	// Add to in-memory storage
	db.storage.Set(key, value)
	
	// Write to log
	err := db.log.Append(persistence.OperationSet, key, value)
	if err != nil {
		// If we fail to log, roll back the in-memory change
		db.storage.Delete(key)
		return NewDatabaseError("set", key, err)
	}
	
	return nil
}

// Get retrieves a value for a given key
func (db *DB) Get(key string) ([]byte, error) {
	// Check if database is closed
	db.mutex.RLock()
	if db.isClosed {
		db.mutex.RUnlock()
		return nil, ErrDatabaseClosed
	}
	db.mutex.RUnlock()
	
	// Input validation
	if key == "" {
		return nil, ErrEmptyKey
	}

	value, exists := db.storage.Get(key)
	if !exists {
		return nil, NewDatabaseError("get", key, ErrKeyNotFound)
	}
	
	return value, nil
}

// Delete removes a key-value pair
func (db *DB) Delete(key string) error {
	// Check if database is closed
	db.mutex.RLock()
	if db.isClosed {
		db.mutex.RUnlock()
		return ErrDatabaseClosed
	}
	db.mutex.RUnlock()
	
	// Input validation
	if key == "" {
		return ErrEmptyKey
	}

	// Check if key exists
	_, exists := db.storage.Get(key)
	if !exists {
		return NewDatabaseError("delete", key, ErrKeyNotFound)
	}

	// Remove from in-memory storage
	db.storage.Delete(key)
	
	// Write to log
	err := db.log.Append(persistence.OperationDelete, key, nil)
	if err != nil {
		return NewDatabaseError("delete", key, err)
	}
	
	return nil
}

// Keys returns all keys in the database
func (db *DB) Keys() []string {
	// Check if database is closed
	db.mutex.RLock()
	if db.isClosed {
		db.mutex.RUnlock()
		return []string{}
	}
	db.mutex.RUnlock()
	
	return db.storage.Keys()
}

// Size returns the number of entries in the database
func (db *DB) Size() int {
	// Check if database is closed
	db.mutex.RLock()
	if db.isClosed {
		db.mutex.RUnlock()
		return 0
	}
	db.mutex.RUnlock()
	
	return db.storage.Size()
}
