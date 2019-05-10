package gokvstores

import (
	"fmt"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"

	conv "github.com/cstockton/go-conv"
)

type PostgresStore struct {
	dbRead  *pg.DB
	dbWrite *pg.DB
}

type KV struct {
	Key   string `sql:",pk"`
	Value string
	Map   map[string]interface{}
	Slice []interface{}
}

// Exists checks if the given key exists.
func (p *PostgresStore) Exists(key string) (bool, error) {
	kv := &KV{Key: key}
	err := p.dbRead.Select(kv)
	fmt.Printf("Exists %s => %#v (%v)\n", key, kv, err)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// MGet returns map of key, value for a list of keys.
func (p *PostgresStore) MGet(keys []string) (map[string]interface{}, error) {
	kvs := []KV{}
	err := p.dbRead.
		Model(&kvs).
		Where("key in (?)", pg.In(keys)).
		Select()
	fmt.Printf("MGet %v => %#v (%v)\n", keys, kvs, err)
	if err != nil {
		return nil, err
	}
	ret := map[string]interface{}{}
	for _, kv := range kvs {
		ret[kv.Key] = kv.Value
	}
	return ret, nil
}

// Get returns value for the given key.
func (p *PostgresStore) Get(key string) (interface{}, error) {
	kv := &KV{Key: key}
	err := p.dbRead.Select(kv)
	fmt.Printf("Get %s => %#v (%v)\n", key, kv, err)
	if err != nil {
		return nil, err
	}
	return kv.Value, nil
}

// Set sets value for the given key.
func (p *PostgresStore) Set(key string, value interface{}) error {
	val, err := conv.String(value)
	if err != nil {
		return err
	}
	kv := &KV{
		Key:   key,
		Value: val,
	}
	_, err = p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("value = EXCLUDED.value").
		Insert()
	fmt.Printf("Set %s => %#v (%v)\n", key, kv, err)
	return err
}

// GetMap returns map for the given key.
func (p *PostgresStore) GetMap(key string) (map[string]interface{}, error) {
	kv := &KV{Key: key}
	err := p.dbRead.Select(kv)
	fmt.Printf("GetMap %s => %#v (%v)\n", key, kv, err)
	if err != nil {
		return nil, err
	}
	return kv.Map, nil
}

// SetMap sets map for the given key.
func (p *PostgresStore) SetMap(key string, value map[string]interface{}) error {
	kv := &KV{
		Key: key,
		Map: value,
	}
	_, err := p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("map = EXCLUDED.map").
		Insert()
	fmt.Printf("SetMap %s => %#v (%v)\n", key, kv, err)
	return err
}

// GetMaps returns maps for the given keys.
func (p *PostgresStore) GetMaps(keys []string) (map[string]map[string]interface{}, error) {
	return nil, nil
}

// SetMaps sets the given maps.
func (p *PostgresStore) SetMaps(maps map[string]map[string]interface{}) error {
	return nil
}

// GetSlice returns slice for the given key.
func (p *PostgresStore) GetSlice(key string) ([]interface{}, error) {
	kv := &KV{Key: key}
	err := p.dbRead.Select(kv)
	fmt.Printf("GetSlice %s => %#v (%v)\n", key, kv, err)
	if err != nil {
		return nil, err
	}
	return kv.Slice, nil
}

// SetSlice sets slice for the given key.
func (p *PostgresStore) SetSlice(key string, value []interface{}) error {
	kv := &KV{
		Key:   key,
		Slice: value,
	}
	_, err := p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("slice = EXCLUDED.slice").
		Insert()
	fmt.Printf("SetSlice %s => %#v (%v)\n", key, kv, err)
	return err
}

// Flush flushes the store.
func (p *PostgresStore) Flush() error {
	return nil
}

// AppendSlice appends values to the given slice.
func (p *PostgresStore) AppendSlice(key string, values ...interface{}) error {
	items, err := p.GetSlice(key)
	if err != nil {
		return err
	}

	for _, item := range values {
		items = append(items, item)
	}
	fmt.Printf("AppendSlice %s => %#v (%v)\n", key, items, err)
	return p.SetSlice(key, items)
}

// Close closes the client connection.
func (p *PostgresStore) Close() error {
	return nil
}

// Delete deletes the given key.
func (p *PostgresStore) Delete(key string) error {
	kv := &KV{Key: key}
	err := p.dbWrite.Delete(kv)
	fmt.Printf("Delete %s => %#v (%v)\n", key, kv, err)
	return err
}

// DeleteMap removes the specified fields from the map stored at key.
func (p *PostgresStore) DeleteMap(key string, fields ...string) error {
	return nil
}

// Keys returns all keys matching pattern
func (p *PostgresStore) Keys(pattern string) ([]interface{}, error) {
	return nil, nil
}

// SetWithExpiration sets the value for the given key for a specified duration.
func (p *PostgresStore) SetWithExpiration(key string, value interface{}, expiration time.Duration) error {
	return nil
}

// NewPostgresStore returns two db connections KVStore.
func NewPostgresStore(readOptions, writeOptions *pg.Options) (KVStore, error) {
	ret := &PostgresStore{
		dbRead:  pg.Connect(readOptions),
		dbWrite: pg.Connect(writeOptions),
	}
	err := createSchema(ret.dbWrite)
	if err != nil {
		fmt.Printf("createSchema: %v\n", err)
	}
	return ret, nil
}

func createSchema(db *pg.DB) error {
	for _, model := range []interface{}{(*KV)(nil)} {
		err := db.CreateTable(model, &orm.CreateTableOptions{
			//Temp: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
