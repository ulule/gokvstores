package gokvstores

import (
	"sort"
	"testing"

	conv "github.com/cstockton/go-conv"
	"github.com/stretchr/testify/assert"
)

func testStore(t *testing.T, store KVStore) {
	is := assert.New(t)

	err := store.Flush()
	is.Nil(err)

	// Set

	err = store.Set("key", "value")
	is.Nil(err)

	// Get

	v, err := store.Get("key")
	is.Equal("value", conv.String(v))

	// Exists

	exists, err := store.Exists("key")
	is.Nil(err)
	is.True(exists)

	// Delete

	err = store.Delete("key")
	is.Nil(err)

	v, _ = store.Get("key")
	is.Nil(v)

	exists, err = store.Exists("key")
	is.Nil(err)
	is.False(exists)

	// Map

	mapResults := map[string]map[string]interface{}{
		"key1": {"language": "go"},
		"key2": {"integer": "1"},
		"key3": {"float": "20.2"},
	}

	for key, expected := range mapResults {
		err = store.SetMap(key, expected)
		is.Nil(err)

		v, err := store.GetMap(key)
		is.Equal(expected, v)

		exists, err := store.Exists(key)
		is.Nil(err)
		is.True(exists)

		err = store.Delete(key)
		is.Nil(err)

		v, _ = store.GetMap(key)
		is.Nil(v)

		exists, err = store.Exists(key)
		is.Nil(err)
		is.False(exists)
	}

	// Slices

	sliceResults := map[string][]interface{}{
		"key1": {"one", "two", "three", "four"},
		"key2": {"1", "2", "3", "4"},
		"key3": {"1.0", "1.1", "1.2", "1.3"},
	}

	for key, expected := range sliceResults {
		err = store.SetSlice(key, expected)
		is.Nil(err)

		v, err := store.GetSlice(key)
		is.Equal(stringSlice(expected), stringSlice(v))

		exists, err := store.Exists(key)
		is.Nil(err)
		is.True(exists)

		err = store.Delete(key)
		is.Nil(err)

		v, _ = store.GetSlice(key)
		is.Nil(v)

		exists, err = store.Exists(key)
		is.Nil(err)
		is.False(exists)
	}
}

func stringSlice(values []interface{}) []string {
	converted := []string{}

	for _, v := range values {
		if v != nil {
			converted = append(converted, conv.String(v))
		}
	}

	sort.Strings(converted)

	return converted
}
