package gokvstores

import (
	"context"
	"fmt"
	"net"
	"time"

	redis "github.com/go-redis/redis/v8"
)

// ----------------------------------------------------------------------------
// Client
// ----------------------------------------------------------------------------

// RedisClient is an interface thats allows to use Redis cluster or a redis single client seamlessly.
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	FlushDB(ctx context.Context) *redis.StatusCmd
	Close() error
	Process(ctx context.Context, cmd redis.Cmder) error
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd
	HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	Pipeline() redis.Pipeliner
}

// RedisPipeline is a struct which contains an opend redis pipeline transaction
type RedisPipeline struct {
	pipeline redis.Pipeliner
}

// RedisClientOptions are Redis client options.
type RedisClientOptions struct {
	Network            string
	Addr               string
	Dialer             func(ctx context.Context, network string, addr string) (net.Conn, error)
	Password           string
	DB                 int
	MaxRetries         int
	DialTimeout        time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
	ReadOnly           bool
}

// RedisClusterOptions are Redis cluster options.
type RedisClusterOptions struct {
	Addrs              []string
	MaxRedirects       int
	ReadOnly           bool
	RouteByLatency     bool
	Password           string
	DialTimeout        time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

// ----------------------------------------------------------------------------
// Store
// ----------------------------------------------------------------------------

// RedisStore is the Redis implementation of KVStore.
type RedisStore struct {
	client     RedisClient
	expiration time.Duration
}

// Get returns value for the given key.
func (r *RedisStore) Get(ctx context.Context, key string) (interface{}, error) {
	cmd := redis.NewCmd(ctx, "get", key)

	if err := r.client.Process(ctx, cmd); err != nil {
		if err == redis.Nil {
			return nil, nil
		}

		return nil, err
	}

	return cmd.Val(), cmd.Err()
}

// MGet returns map of key, value for a list of keys.
func (r *RedisStore) MGet(ctx context.Context, keys []string) (map[string]interface{}, error) {
	values, err := r.client.MGet(ctx, keys...).Result()

	newValues := make(map[string]interface{}, len(keys))

	for k, v := range keys {
		value := values[k]
		if err != nil {
			return nil, err
		}

		newValues[v] = value
	}
	return newValues, nil
}

// Set sets the value for the given key.
func (r *RedisStore) Set(ctx context.Context, key string, value interface{}) error {
	return r.client.Set(ctx, key, value, r.expiration).Err()
}

// SetWithExpiration sets the value for the given key.
func (r *RedisStore) SetWithExpiration(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// GetMap returns map for the given key.
func (r *RedisStore) GetMap(ctx context.Context, key string) (map[string]interface{}, error) {
	values, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	newValues := make(map[string]interface{}, len(values))
	for k, v := range values {
		newValues[k] = v
	}

	return newValues, nil
}

// SetMap sets map for the given key.
func (r *RedisStore) SetMap(ctx context.Context, key string, values map[string]interface{}) error {
	newValues := make(map[string]string, len(values))

	for k, v := range values {
		switch vv := v.(type) {
		case string:
			newValues[k] = vv
		default:
			newValues[k] = fmt.Sprintf("%v", vv)
		}
	}

	return r.client.HMSet(ctx, key, newValues).Err()
}

// DeleteMap removes the specified fields from the map stored at key.
func (r *RedisStore) DeleteMap(ctx context.Context, key string, fields ...string) error {
	return r.client.HDel(ctx, key, fields...).Err()
}

// GetSlice returns slice for the given key.
func (r *RedisStore) GetSlice(ctx context.Context, key string) ([]interface{}, error) {
	values, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	newValues := make([]interface{}, len(values))
	for i := range values {
		newValues[i] = values[i]
	}

	return newValues, nil
}

// SetSlice sets map for the given key.
func (r *RedisStore) SetSlice(ctx context.Context, key string, values []interface{}) error {
	for _, v := range values {
		if v != nil {
			if err := r.client.SAdd(ctx, key, v).Err(); err != nil {
				return err
			}
		}
	}

	return nil
}

// AppendSlice appends values to the given slice.
func (r *RedisStore) AppendSlice(ctx context.Context, key string, values ...interface{}) error {
	return r.SetSlice(ctx, key, values)
}

// Exists checks key existence.
func (r *RedisStore) Exists(ctx context.Context, keys ...string) (bool, error) {
	cmd := r.client.Exists(ctx, keys...)
	return cmd.Val() > 0, cmd.Err()
}

// Delete deletes key.
func (r *RedisStore) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// Keys returns all keys matching pattern.
func (r *RedisStore) Keys(ctx context.Context, pattern string) ([]interface{}, error) {
	values, err := r.client.Keys(ctx, pattern).Result()

	if len(values) == 0 {
		return nil, err
	}

	newValues := make([]interface{}, len(values))

	for k, v := range values {
		newValues[k] = v
	}

	return newValues, err
}

// Flush flushes the current database.
func (r *RedisStore) Flush(ctx context.Context) error {
	return r.client.FlushDB(ctx).Err()
}

// Close closes the client connection.
func (r *RedisStore) Close() error {
	return r.client.Close()
}

// NewRedisClientStore returns Redis client instance of KVStore.
func NewRedisClientStore(ctx context.Context, options *RedisClientOptions, expiration time.Duration) (KVStore, error) {
	opts := &redis.Options{
		Network:            options.Network,
		Addr:               options.Addr,
		Dialer:             options.Dialer,
		Password:           options.Password,
		DB:                 options.DB,
		MaxRetries:         options.MaxRetries,
		DialTimeout:        options.DialTimeout,
		ReadTimeout:        options.ReadTimeout,
		WriteTimeout:       options.WriteTimeout,
		PoolSize:           options.PoolSize,
		PoolTimeout:        options.PoolTimeout,
		IdleTimeout:        options.IdleTimeout,
		IdleCheckFrequency: options.IdleCheckFrequency,
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &RedisStore{
		client:     client,
		expiration: expiration,
	}, nil
}

// NewRedisClusterStore returns Redis cluster client instance of KVStore.
func NewRedisClusterStore(ctx context.Context, options *RedisClusterOptions, expiration time.Duration) (KVStore, error) {
	opts := &redis.ClusterOptions{
		Addrs:              options.Addrs,
		MaxRedirects:       options.MaxRedirects,
		ReadOnly:           options.ReadOnly,
		RouteByLatency:     options.RouteByLatency,
		Password:           options.Password,
		DialTimeout:        options.DialTimeout,
		ReadTimeout:        options.ReadTimeout,
		WriteTimeout:       options.WriteTimeout,
		PoolSize:           options.PoolSize,
		PoolTimeout:        options.PoolTimeout,
		IdleTimeout:        options.IdleTimeout,
		IdleCheckFrequency: options.IdleCheckFrequency,
	}

	client := redis.NewClusterClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return &RedisStore{
		client:     client,
		expiration: expiration,
	}, nil
}

// Pipeline uses pipeline as a Redis client to execute multiple calls at once
func (r *RedisStore) Pipeline(ctx context.Context, f func(r *RedisStore) error) ([]redis.Cmder, error) {
	pipe := r.client.Pipeline()

	redisPipeline := RedisPipeline{
		pipeline: pipe,
	}

	store := &RedisStore{
		client:     redisPipeline,
		expiration: r.expiration,
	}

	err := f(store)
	if err != nil {
		return nil, err
	}

	cmds, err := pipe.Exec(ctx)
	return cmds, err
}

// GetMaps returns maps for the given keys.
func (r *RedisStore) GetMaps(ctx context.Context, keys []string) (map[string]map[string]interface{}, error) {
	commands, err := r.Pipeline(ctx, func(r *RedisStore) error {
		for _, key := range keys {
			r.client.HGetAll(ctx, key)
		}
		return nil

	})
	if err != nil {
		return nil, err
	}

	newValues := make(map[string]map[string]interface{}, len(keys))

	for i, key := range keys {
		cmd := commands[i]
		values, _ := cmd.(*redis.StringStringMapCmd).Result()
		if values != nil {
			valueMap := make(map[string]interface{}, len(values))
			for k, v := range values {
				valueMap[k] = v
			}

			newValues[key] = valueMap
		} else {
			newValues[key] = nil
		}
	}

	return newValues, nil
}

// SetMaps sets the given maps.
func (r *RedisStore) SetMaps(ctx context.Context, maps map[string]map[string]interface{}) error {
	_, err := r.Pipeline(ctx, func(r *RedisStore) error {
		for k, v := range maps {
			r.SetMap(ctx, k, v)
		}
		return nil

	})
	return err
}

// Pipeline returns Redis pipeline
func (r RedisPipeline) Pipeline() redis.Pipeliner {
	return r.pipeline
}

// Ping implements RedisClient Ping for pipeline
func (r RedisPipeline) Ping(ctx context.Context) *redis.StatusCmd {
	return r.pipeline.Ping(ctx)
}

// Exists implements RedisClient Exists for pipeline
func (r RedisPipeline) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return r.pipeline.Exists(ctx, keys...)
}

// Del implements RedisClient Del for pipeline
func (r RedisPipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return r.pipeline.Del(ctx, keys...)
}

// FlushDb implements RedisClient FlushDb for pipeline
func (r RedisPipeline) FlushDB(ctx context.Context) *redis.StatusCmd {
	return r.pipeline.FlushDB(ctx)
}

// Close implements RedisClient Close for pipeline
func (r RedisPipeline) Close() error {
	return r.pipeline.Close()
}

// Process implements RedisClient Process for pipeline
func (r RedisPipeline) Process(ctx context.Context, cmd redis.Cmder) error {
	return r.pipeline.Process(ctx, cmd)
}

// Get implements RedisClient Get for pipeline
func (r RedisPipeline) Get(ctx context.Context, key string) *redis.StringCmd {
	return r.pipeline.Get(ctx, key)
}

// MGet implements RedisClient MGet for pipeline
func (r RedisPipeline) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return r.pipeline.MGet(ctx, keys...)
}

// Set implements RedisClient Set for pipeline
func (r RedisPipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return r.pipeline.Set(ctx, key, value, expiration)
}

// HDel implements RedisClient HDel for pipeline
func (r RedisPipeline) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return r.pipeline.HDel(ctx, key, fields...)
}

// HGetAll implements RedisClient HGetAll for pipeline
func (r RedisPipeline) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	return r.pipeline.HGetAll(ctx, key)
}

// HMSet implements RedisClient HMSet for pipeline
func (r RedisPipeline) HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
	return r.pipeline.HMSet(ctx, key, values...)
}

// SMembers implements RedisClient SMembers for pipeline
func (r RedisPipeline) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return r.pipeline.SMembers(ctx, key)
}

// SAdd implements RedisClient SAdd for pipeline
func (r RedisPipeline) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	return r.pipeline.SAdd(ctx, key, members...)
}

// Keys implements RedisClient Keys for pipeline
func (r RedisPipeline) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	return r.pipeline.Keys(ctx, pattern)
}

var _ KVStore = &RedisStore{}
