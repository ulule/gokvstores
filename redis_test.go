package gokvstores

import (
	"sort"
	"testing"
	"time"

	conv "github.com/cstockton/go-conv"
	"github.com/stretchr/testify/assert"
)

func TestRedisStore(t *testing.T) {
	store, err := NewRedisClientStore(&RedisClientOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}, time.Second*30)

	assert.Nil(t, err)

	testStore(t, store)

	is := assert.New(t)

	mapResults := map[string]map[string]interface{}{
		"key1": {"language": "go"},
		"key2": {"integer": "1"},
		"key3": {"float": "20.2"},
	}
	expectedStrings := []string{"key1", "key2", "key3"}

	for key, expected := range mapResults {
		err = store.SetMap(key, expected)
		is.Nil(err)
	}

	values, err := store.Keys("key*")
	is.Nil(err)

	sort.Strings(expectedStrings)
	result := make([]string, len(values))
	for _, v := range values {
		result = append(result, conv.String(v))
	}
	sort.Strings(result)

	is.Equal(expectedStrings, result)

	assert.Nil(t, store.Close())
}
