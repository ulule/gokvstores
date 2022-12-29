package gokvstores

import (
	"context"
	"time"
)

// DummyStore is a noop store (caching disabled).
type DummyStore struct{}

// Get returns value for the given key.
func (DummyStore) Get(ctx context.Context, key string) (interface{}, error) {
	return nil, nil
}

// MGet returns map of key, value for a list of keys.
func (DummyStore) MGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	return nil, nil
}

// Set sets value for the given key.
func (DummyStore) Set(ctx context.Context, key string, value interface{}) error {
	return nil
}

// SetWithExpiration sets the value for the given key for a specified duration.
func (DummyStore) SetWithExpiration(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return nil
}

// GetMap returns map for the given key.
func (DummyStore) GetMap(ctx context.Context, key string) (map[string]interface{}, error) {
	return nil, nil
}

// GetMaps returns maps for the given keys.
func (DummyStore) GetMaps(ctx context.Context, keys []string) (map[string]map[string]interface{}, error) {
	return nil, nil
}

// SetMap sets map for the given key.
func (DummyStore) SetMap(ctx context.Context, key string, value map[string]interface{}) error {
	return nil
}

// SetMaps sets the given maps.
func (DummyStore) SetMaps(ctx context.Context, maps map[string]map[string]interface{}) error {
	return nil
}

// DeleteMap removes the specified fields from the map stored at key.
func (DummyStore) DeleteMap(ctx context.Context, key string, fields ...string) error { return nil }

// GetSlice returns slice for the given key.
func (DummyStore) GetSlice(ctx context.Context, key string) ([]interface{}, error) {
	return nil, nil
}

// SetSlice sets slice for the given key.
func (DummyStore) SetSlice(ctx context.Context, key string, value []interface{}) error {
	return nil
}

// AppendSlice appends values to an existing slice.
// If key does not exist, creates slice.
func (DummyStore) AppendSlice(ctx context.Context, key string, values ...interface{}) error {
	return nil
}

// Exists checks if the given key exists.
func (DummyStore) Exists(ctx context.Context, keys ...string) (bool, error) {
	return false, nil
}

// Delete deletes the given key.
func (DummyStore) Delete(ctx context.Context, key string) error {
	return nil
}

// Keys returns all keys matching pattern
func (DummyStore) Keys(ctx context.Context, pattern string) ([]interface{}, error) {
	return nil, nil
}

// Flush flushes the store.
func (DummyStore) Flush(ctx context.Context) error {
	return nil
}

// Close closes the connection to the store.
func (DummyStore) Close() error {
	return nil
}

var _ KVStore = &DummyStore{}
