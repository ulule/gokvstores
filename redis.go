package gokvstores

import (
	"time"

	redis "gopkg.in/redis.v5"
)

// ----------------------------------------------------------------------------
// Client
// ----------------------------------------------------------------------------

// RedisClient is an interface thats allows to use Redis cluster or a redis single client seamlessly.
type RedisClient interface {
	Ping() *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SAdd(key string, members ...interface{}) *redis.IntCmd
	SMembers(key string) *redis.StringSliceCmd
	Exists(key string) *redis.BoolCmd
	Append(key, value string) *redis.IntCmd
	Del(keys ...string) *redis.IntCmd
	FlushDb() *redis.StatusCmd
	Close() error
	Process(cmd redis.Cmder) error
}

// ----------------------------------------------------------------------------
// KVStore
// ----------------------------------------------------------------------------

// RedisKVStore is the Redis implementation of KVStore.
type RedisKVStore struct {
	client     RedisClient
	expiration time.Duration
}

func (r *RedisKVStore) Get(key string) interface{} {
	cmd := redis.NewCmd("GET", key)
	r.client.Process(cmd)
	return cmd.Val()
}

func (r *RedisKVStore) Set(key string, value interface{}) error {
	return r.client.Set(key, value, r.expiration).Err()
}

func (r *RedisKVStore) SetAdd(key string, value interface{}) error {
	return r.client.SAdd(key, value).Err()
}

func (r *RedisKVStore) SetMembers(key string) []interface{} {
	vals := r.client.SMembers(key).Val()
	if len(vals) == 0 {
		return nil
	}

	newVals := make([]interface{}, len(vals))
	for i, v := range vals {
		newVals[i] = v
	}

	return newVals
}

func (r *RedisKVStore) Append(key string, value interface{}) error {
	cmd := redis.NewIntCmd("append", key, value)
	r.client.Process(cmd)
	return cmd.Err()
}

func (r *RedisKVStore) Close() error {
	return r.client.Close()
}

func (r *RedisKVStore) Flush() error {
	return r.client.FlushDb().Err()
}

func (r *RedisKVStore) Exists(key string) bool {
	return r.client.Exists(key).Val()
}

func (r *RedisKVStore) Delete(key string) error {
	return r.client.Del(key).Err()
}

// ----------------------------------------------------------------------------
// Initializers
// ----------------------------------------------------------------------------

// NewRedisClientKVStore returns Redis client instance of KVStore.
func NewRedisClientKVStore(options *redis.Options, expiration int) (KVStore, error) {
	client := redis.NewClient(options)

	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &RedisKVStore{
		client:     client,
		expiration: time.Duration(expiration) * time.Second,
	}, nil
}

// NewRedisClusterKVStore returns Redis cluster client instance of KVStore.
func NewRedisClusterKVStore(options *redis.ClusterOptions, expiration int) (KVStore, error) {
	client := redis.NewClusterClient(options)

	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &RedisKVStore{
		client:     client,
		expiration: time.Duration(expiration) * time.Second,
	}, nil
}
