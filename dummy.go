package gokvstores

// DummyStore is a noop store (caching disabled).
type DummyStore struct{}

// Get returns value for the given key.
func (s DummyStore) Get(key string) (interface{}, error) {
	return nil, nil
}

// Set sets value for the given key.
func (s DummyStore) Set(key string, value interface{}) error {
	return nil
}

// GetMap returns map for the given key.
func (s DummyStore) GetMap(key string) (map[string]interface{}, error) {
	return nil, nil
}

// SetMap sets map for the given key.
func (s DummyStore) SetMap(key string, value map[string]interface{}) error {
	return nil
}

// GetSlice returns slice for the given key.
func (s DummyStore) GetSlice(key string) ([]interface{}, error) {
	return nil, nil
}

// SetSlice sets slice for the given key.
func (s DummyStore) SetSlice(key string, value []interface{}) error {
	return nil
}

// AppendSlice appends values to an existing slice.
// If key does not exist, creates slice.
func (s DummyStore) AppendSlice(key string, values ...interface{}) error {
	return nil
}

// Exists checks if the given key exists.
func (s DummyStore) Exists(key string) (bool, error) {
	return false, nil
}

// Delete deletes the given key.
func (s DummyStore) Delete(key string) error {
	return nil
}

// Return all keys matching pattern
func (c *DummyStore) Keys(pattern string) ([]interface{}, error) {
	return nil, nil
}

// Flush flushes the store.
func (s DummyStore) Flush() error {
	return nil
}

// Close closes the connection to the store.
func (s DummyStore) Close() error {
	return nil
}
