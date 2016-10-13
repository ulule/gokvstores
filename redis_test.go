package gokvstores

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	store, err := NewRedisClientKVStore(&RedisClientOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}, time.Second*30)

	assert.Nil(t, err)

	testStore(t, store)

	assert.Nil(t, store.Close())
}
