package gokvstores

import (
	"errors"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"

	conv "github.com/cstockton/go-conv"
)

var (
	errorExpired = errors.New("Key is expired")
)

type PostgresStore struct {
	dbWrite *pg.DB
	dbRead  *pg.DB
}

type KV struct {
	tableName struct{} `sql:"gokvstores_data"`
	Key       string   `sql:",pk"`
	Value     string
	Map       map[string]interface{}
	Slice     []interface{}
	CreatedAt time.Time `sql:"default:now()"`
	ExpiresAt time.Time
}

// Exists checks if the given key exists.
func (p *PostgresStore) Exists(key string) (bool, error) {
	kv := &KV{
		Key: key,
	}
	err := p.dbRead.Select(kv)
	//TODO: change to count and return false not only on error
	if err != nil {
		return false, nil
	}
	if (kv.ExpiresAt != time.Time{}) && kv.ExpiresAt.Before(time.Now()) {
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
	if err != nil {
		return nil, err
	}
	if (kv.ExpiresAt != time.Time{}) && kv.ExpiresAt.Before(time.Now()) {
		return nil, errorExpired
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
		Key:       key,
		Value:     val,
		ExpiresAt: time.Time{},
	}
	_, err = p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("value = EXCLUDED.value, expires_at = EXCLUDED.expires_at").
		Insert()
	return err
}

// GetMap returns map for the given key.
func (p *PostgresStore) GetMap(key string) (map[string]interface{}, error) {
	kv := &KV{Key: key}
	err := p.dbRead.Select(kv)
	if err != nil {
		return nil, err
	}
	return kv.Map, nil
}

// SetMap sets map for the given key.
func (p *PostgresStore) SetMap(key string, value map[string]interface{}) error {
	kv := &KV{
		Key:       key,
		Map:       value,
		ExpiresAt: time.Time{},
	}
	_, err := p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("map = EXCLUDED.map, expires_at = EXCLUDED.expires_at").
		Insert()
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
	if err != nil {
		return nil, err
	}
	return kv.Slice, nil
}

// SetSlice sets slice for the given key.
func (p *PostgresStore) SetSlice(key string, value []interface{}) error {
	kv := &KV{
		Key:       key,
		Slice:     value,
		ExpiresAt: time.Time{},
	}
	_, err := p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("slice = EXCLUDED.slice, expires_at = EXCLUDED.expires_at").
		Insert()
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

	//TODO: find the way to do it as SQL wrapper array functions
	for _, item := range values {
		items = append(items, item)
	}
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
	val, err := conv.String(value)
	if err != nil {
		return err
	}
	kv := &KV{
		Key:   key,
		Value: val,
		//TODO: find the way to do it as SQL wrapper `UPDATE ... now() + interfal '12 seconds'`
		ExpiresAt: time.Now().Add(expiration),
	}
	_, err = p.dbWrite.Model(kv).
		OnConflict("(key) DO UPDATE").
		Set("value = EXCLUDED.value, expires_at = EXCLUDED.expires_at").
		Insert()
	return err
}

func (p *PostgresStore) createSchema() error {
	for _, model := range []interface{}{(*KV)(nil)} {
		err := p.dbWrite.CreateTable(model, &orm.CreateTableOptions{
			//Temp: true,
		})
		if err != nil {
			return err
		}
	}

	_, err := p.dbWrite.Exec(`CREATE FUNCTION gokvstores_data_delete_old_rows() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
	BEGIN
		DELETE FROM gokvstores_data WHERE expires_at < NOW();
		RETURN NEW;
	END;
	$$;`)
	if err != nil {
		return err
	}

	_, err = p.dbWrite.Exec(`CREATE TRIGGER gokvstores_data_delete_old_rows_trigger
    AFTER INSERT ON gokvstores_data
    EXECUTE PROCEDURE gokvstores_data_delete_old_rows();`)
	if err != nil {
		return err
	}
	return nil
}

// NewPostgresStore returns two db connections KVStore.
func NewPostgresStore(writeUrl, readUrl string, initSchema bool) (KVStore, error) {
	readOptions, err := pg.ParseURL(readUrl)
	if err != nil {
		return nil, err
	}
	writeOptions, err := pg.ParseURL(writeUrl)
	if err != nil {
		return nil, err
	}
	return NewPostgresStoreConn(pg.Connect(writeOptions), pg.Connect(readOptions), initSchema)
}

func NewPostgresStoreConn(writeConn, readConn *pg.DB, initSchema bool) (KVStore, error) {
	ret := &PostgresStore{
		dbWrite: writeConn,
		dbRead:  readConn,
	}
	if initSchema {
		ret.createSchema()
	}
	return ret, nil
}
