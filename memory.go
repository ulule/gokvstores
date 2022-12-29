package gokvstores

import (
	"context"
	"time"

	"github.com/patrickmn/go-cache"
)

// MemoryStore is the in-memory implementation of KVStore.
type MemoryStore struct {
	cache           *cache.Cache
	expiration      time.Duration
	cleanupInterval time.Duration
}

// Get returns item from the cache.
func (c *MemoryStore) Get(ctx context.Context, key string) (interface{}, error) {
	item, _ := c.cache.Get(key)
	return item, nil
}

// MGet returns map of key, value for a list of keys.
func (c *MemoryStore) MGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	results := make(map[string]interface{}, len(keys))
	for _, key := range keys {
		item, _ := c.Get(ctx, key)
		results[key] = item
	}
	return results, nil
}

// Set sets value in the cache.
func (c *MemoryStore) Set(ctx context.Context, key string, value interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// SetWithExpiration sets the value for the given key for a specified duration.
func (c *MemoryStore) SetWithExpiration(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	c.cache.Set(key, value, expiration)
	return nil
}

// GetMap returns map for the given key.
func (c *MemoryStore) GetMap(ctx context.Context, key string) (map[string]interface{}, error) {
	if v, found := c.cache.Get(key); found {
		return v.(map[string]interface{}), nil
	}
	return nil, nil
}

// GetMaps returns maps for the given keys.
func (c *MemoryStore) GetMaps(ctx context.Context, keys []string) (map[string]map[string]interface{}, error) {
	values := make(map[string]map[string]interface{}, len(keys))
	for _, v := range keys {
		value, _ := c.GetMap(ctx, v)
		if value != nil {
			values[v] = value
		}
	}

	return values, nil
}

// SetMap sets a map for the given key.
func (c *MemoryStore) SetMap(ctx context.Context, key string, value map[string]interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// SetMaps sets the given maps.
func (c *MemoryStore) SetMaps(ctx context.Context, maps map[string]map[string]interface{}) error {
	for k, v := range maps {
		c.SetMap(ctx, k, v)
	}
	return nil
}

// DeleteMap removes the specified fields from the map stored at key.
func (c *MemoryStore) DeleteMap(ctx context.Context, key string, fields ...string) error {
	m, err := c.GetMap(ctx, key)
	if err != nil {
		return err
	}

	for _, field := range fields {
		delete(m, field)
	}

	return c.SetMap(ctx, key, m)
}

// GetSlice returns slice for the given key.
func (c *MemoryStore) GetSlice(ctx context.Context, key string) ([]interface{}, error) {
	if v, found := c.cache.Get(key); found {
		return v.([]interface{}), nil
	}
	return nil, nil
}

// SetSlice sets slice for the given key.
func (c *MemoryStore) SetSlice(ctx context.Context, key string, value []interface{}) error {
	c.cache.Set(key, value, c.expiration)
	return nil
}

// AppendSlice appends values to the given slice.
func (c *MemoryStore) AppendSlice(ctx context.Context, key string, values ...interface{}) error {
	items, err := c.GetSlice(ctx, key)
	if err != nil {
		return err
	}

	if items == nil {
		return c.SetSlice(ctx, key, values)
	}

	for _, item := range values {
		items = append(items, item)
	}

	return c.cache.Replace(key, items, c.expiration)
}

// Close does nothing for this backend.
func (c *MemoryStore) Close() error {
	return nil
}

// Flush removes all items from the cache.
func (c *MemoryStore) Flush(ctx context.Context) error {
	c.cache.Flush()
	return nil
}

// Delete deletes the given key.
func (c *MemoryStore) Delete(ctx context.Context, key string) error {
	c.cache.Delete(key)
	return nil
}

// Keys returns all keys matching pattern
func (c *MemoryStore) Keys(ctx context.Context, pattern string) ([]interface{}, error) {
	return nil, nil
}

// Exists checks if the given key exists.
func (c *MemoryStore) Exists(ctx context.Context, keys ...string) (bool, error) {
	for i := range keys {
		if _, exists := c.cache.Get(keys[i]); !exists {
			return false, nil
		}
	}
	return true, nil
}

// NewMemoryStore returns in-memory KVStore.
func NewMemoryStore(expiration time.Duration, cleanupInterval time.Duration) (KVStore, error) {
	return &MemoryStore{
		cache:           cache.New(expiration, cleanupInterval),
		expiration:      time.Duration(expiration) * time.Second,
		cleanupInterval: cleanupInterval,
	}, nil
}
