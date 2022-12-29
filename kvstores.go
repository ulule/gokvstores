package gokvstores

import (
	"context"
	"sort"
	"time"
)

// KVStore is the KV store interface.
type KVStore interface {
	// Get returns value for the given key.
	Get(ctx context.Context, key string) (interface{}, error)

	// MGet returns map of key, value for a list of keys.
	MGet(ctx context.Context, keys []string) (map[string]interface{}, error)

	// Set sets value for the given key.
	Set(ctx context.Context, key string, value interface{}) error

	// SetWithExpiration sets the value for the given key for a specified duration.
	SetWithExpiration(ctx context.Context, key string, value interface{}, expiration time.Duration) error

	// GetMap returns map for the given key.
	GetMap(ctx context.Context, key string) (map[string]interface{}, error)

	// GetMaps returns maps for the given keys.
	GetMaps(ctx context.Context, keys []string) (map[string]map[string]interface{}, error)

	// SetMap sets map for the given key.
	SetMap(ctx context.Context, key string, value map[string]interface{}) error

	// SetMaps sets the given maps.
	SetMaps(ctx context.Context, maps map[string]map[string]interface{}) error

	// DeleteMap removes the specified fields from the map stored at key.
	DeleteMap(ctx context.Context, key string, fields ...string) error

	// GetSlice returns slice for the given key.
	GetSlice(ctx context.Context, key string) ([]interface{}, error)

	// SetSlice sets slice for the given key.
	SetSlice(ctx context.Context, key string, value []interface{}) error

	// AppendSlice appends values to an existing slice.
	// If key does not exist, creates slice.
	AppendSlice(ctx context.Context, key string, values ...interface{}) error

	// Exists checks if the given key exists.
	Exists(ctx context.Context, keys ...string) (bool, error)

	// Delete deletes the given key.
	Delete(ctx context.Context, key string) error

	// Flush flushes the store.
	Flush(ctx context.Context) error

	// Return all keys matching pattern
	Keys(ctx context.Context, pattern string) ([]interface{}, error)

	// Close closes the connection to the store.
	Close() error
}

func stringSlice(values []interface{}) ([]string, error) {
	converted := []string{}

	for _, v := range values {
		if v != nil {
			val, ok := v.(string)
			if ok {
				converted = append(converted, val)
			}
		}
	}

	sort.Strings(converted)

	return converted, nil
}
