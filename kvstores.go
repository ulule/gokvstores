package gokvstores

// KVStore is the KV store interface.
type KVStore interface {

	// Get returns value for the given key.
	Get(key string) (interface{}, error)

	// Set sets value for the given key.
	Set(key string, value interface{}) error

	// GetMap map for the given key.
	GetMap(key string) (map[string]interface{}, error)

	// SetMap sets map for the given key.
	SetMap(key string, value map[string]interface{}) error

	// GetSlice returns slice for the given key.
	GetSlice(key string) ([]interface{}, error)

	// SetSlice sets slice for the given key.
	SetSlice(key string, value []interface{}) error

	// Exists checks if the given key exists.
	Exists(key string) (bool, error)

	// Delete deletes value for the given key.
	Delete(key string) error

	// Flush flushes the store.
	Flush() error

	// Close closes the connection to the store.
	Close() error
}
