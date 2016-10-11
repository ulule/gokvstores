package gokvstores

type KVStore interface {
	// Get returns the value for the given key.
	Get(key string) interface{}

	// Set sets a value for the given key.
	Set(key string, value interface{}) error

	// SetAdd adds a value to the set stored under the key,
	// creates a new set if one doesn't exist. Evicts an old item if necessary.
	SetAdd(key string, value interface{}) error

	// SetMembers returns the members of the set. It will return nil if there is
	// no such set, or if the item is not a set.
	SetMembers(key string) []interface{}

	// Append appends value to the given key values.
	Append(key string, value interface{}) error

	// Exists checks if the given key exists.
	Exists(key string) bool

	// Delete deletes value for the given key.
	Delete(key string) error

	// Flush flushes the store.
	Flush() error

	// Close closes the connection to the store.
	Close() error
}
