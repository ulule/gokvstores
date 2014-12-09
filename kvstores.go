package kvstores

type KVStore interface {
	Connection() KVStoreConnection
	Close() error
}

type KVStoreConnection interface {
	Close() error
	Get(key string) string
	Delete(key string) error
	Flush() error
	Exists(key string) bool
	Set(key string, value string) error
}
