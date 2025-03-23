package storage

import (
	"sync"
)

// HashTable implements an in-memory key-value store with thread safety
type HashTable struct {
	buckets    []*Bucket
	bucketSize int
	mutex      sync.RWMutex
}

// Bucket holds entries for a portion of the key space
type Bucket struct {
	entries map[string][]byte
	mutex   sync.RWMutex // Fine-grained locking
}

// NewHashTable creates a new hash table with specified bucket count
func NewHashTable(numBuckets int) *HashTable {
	buckets := make([]*Bucket, numBuckets)
	for i := 0; i < numBuckets; i++ {
		buckets[i] = &Bucket{
			entries: make(map[string][]byte),
		}
	}
	return &HashTable{
		buckets:    buckets,
		bucketSize: numBuckets,
	}
}

// hash determines which bucket a key belongs to
func (ht *HashTable) hash(key string) int {
	hash := 0
	for _, char := range key {
		hash += int(char)
	}
	return hash % ht.bucketSize
}

// Set stores a value for a given key
func (ht *HashTable) Set(key string, value []byte) {
	bucketIndex := ht.hash(key)
	bucket := ht.buckets[bucketIndex]
	
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	
	bucket.entries[key] = value
}

// Get retrieves a value for a given key
func (ht *HashTable) Get(key string) ([]byte, bool) {
	bucketIndex := ht.hash(key)
	bucket := ht.buckets[bucketIndex]
	
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()
	
	value, exists := bucket.entries[key]
	return value, exists
}

// Delete removes a key-value pair
func (ht *HashTable) Delete(key string) bool {
	bucketIndex := ht.hash(key)
	bucket := ht.buckets[bucketIndex]
	
	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()
	
	_, exists := bucket.entries[key]
	if exists {
		delete(bucket.entries, key)
		return true
	}
	return false
}

// Keys returns all keys in the hash table
func (ht *HashTable) Keys() []string {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	
	keys := []string{}
	for _, bucket := range ht.buckets {
		bucket.mutex.RLock()
		for k := range bucket.entries {
			keys = append(keys, k)
		}
		bucket.mutex.RUnlock()
	}
	return keys
}

// Size returns the number of entries in the hash table
func (ht *HashTable) Size() int {
	count := 0
	
	for _, bucket := range ht.buckets {
		bucket.mutex.RLock()
		count += len(bucket.entries)
		bucket.mutex.RUnlock()
	}
	
	return count
}
