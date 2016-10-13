package gokvstores

import (
	"time"

	"github.com/patrickmn/go-cache"
)

// CacheKVStore is the in-memory implementation of KVStore.
type CacheKVStore struct {
	cache           *cache.Cache
	expiration      time.Duration
	cleanupInterval time.Duration
}

// Get returns item from the cache.
func (c *CacheKVStore) Get(key string) (interface{}, error) {
	item, _ := c.cache.Get(key)
	return item, nil
}

// Set sets value in the cache.
func (c *CacheKVStore) Set(key string, value interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// GetMap returns map for the given key.
func (c *CacheKVStore) GetMap(key string) (map[string]interface{}, error) {
	if v, found := c.cache.Get(key); found {
		return v.(map[string]interface{}), nil
	}
	return nil, nil
}

// SetMap sets a map for the given key.
func (c *CacheKVStore) SetMap(key string, value map[string]interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// GetSlice returns slice for the given key.
func (c *CacheKVStore) GetSlice(key string) ([]interface{}, error) {
	if v, found := c.cache.Get(key); found {
		return v.([]interface{}), nil
	}
	return nil, nil
}

// SetSlice sets slice for the given key.
func (c *CacheKVStore) SetSlice(key string, value []interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// Close does nothing for this backend.
func (c *CacheKVStore) Close() error {
	return nil
}

// Flush removes all items from the cache.
func (c *CacheKVStore) Flush() error {
	c.cache.Flush()
	return nil
}

// Delete deletes the given key.
func (c *CacheKVStore) Delete(key string) error {
	c.cache.Delete(key)
	return nil
}

// Exists checks if the given key exists.
func (c *CacheKVStore) Exists(key string) (bool, error) {
	if _, exists := c.cache.Get(key); exists {
		return true, nil
	}
	return false, nil
}

// ----------------------------------------------------------------------------
// Initializers
// ----------------------------------------------------------------------------

// NewCacheKVStore returns in-memory KVStore.
func NewCacheKVStore(expiration time.Duration, cleanupInterval time.Duration) (KVStore, error) {
	return &CacheKVStore{
		cache:           cache.New(expiration, cleanupInterval),
		expiration:      time.Duration(expiration) * time.Second,
		cleanupInterval: cleanupInterval,
	}, nil
}
