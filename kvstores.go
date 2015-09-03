package gokvstores

type KVStore interface {
	Connection() KVStoreConnection
	Close() error
}

type KVStoreConnection interface {
	Close() error
	Get(key string) interface{}
	Delete(key string) error
	Flush() error
	Exists(key string) bool
	Set(key string, value interface{}) error
	SetAdd(key string, value interface{}) error
	SetMembers(key string) []interface{}
}
