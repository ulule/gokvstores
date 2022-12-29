package gokvstores

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testStore(t *testing.T, store KVStore) {
	is := assert.New(t)
	ctx := context.Background()

	err := store.Flush(ctx)
	is.NoError(err)

	itemResults := map[string]interface{}{
		"key1": "1",
		"key2": "2",
		"key3": "3",
	}

	for key, expected := range itemResults {
		// Set

		err = store.Set(ctx, key, expected)
		is.NoError(err)

		// Get

		v, err := store.Get(ctx, key)
		is.NoError(err)
		val, ok := v.(string)
		is.True(ok)
		is.Equal(expected, val)

		exists, err := store.Exists(ctx, key)
		is.NoError(err)
		is.True(exists)

	}

	keys := []string{"key1", "key2", "key3"}

	mResults, err := store.MGet(ctx, keys)

	for key, result := range mResults {
		val, ok := result.(string)
		is.True(ok)
		is.Equal(val, itemResults[key])
	}

	for key := range itemResults {
		// Delete

		err = store.Delete(ctx, key)
		is.NoError(err)

		v, _ := store.Get(ctx, key)
		is.Nil(v)

		exists, err := store.Exists(ctx, key)
		is.NoError(err)
		is.False(exists)
	}

	// Map

	mapResults := map[string]map[string]interface{}{
		"key1": {"language": "go"},
		"key2": {"integer": "1"},
		"key3": {"float": "20.2"},
	}

	for key, expected := range mapResults {
		err = store.SetMap(ctx, key, expected)
		is.NoError(err)

		v, err := store.GetMap(ctx, key)
		is.Equal(expected, v)

		exists, err := store.Exists(ctx, key)
		is.NoError(err)
		is.True(exists)
	}

	results, err := store.GetMaps(ctx, keys)
	is.NoError(err)

	for key, result := range results {
		is.Equal(result, mapResults[key])
	}

	for key := range mapResults {
		err = store.Delete(ctx, key)
		is.NoError(err)

		v, _ := store.GetMap(ctx, key)
		is.Nil(v)

		exists, err := store.Exists(ctx, key)
		is.NoError(err)
		is.False(exists)
	}

	is.NoError(store.SetMaps(ctx, mapResults))
	results, err = store.GetMaps(ctx, keys)
	is.NoError(err)
	for key, result := range results {
		is.Equal(result, mapResults[key])
		is.NoError(store.Delete(ctx, key))
	}

	// Slices

	sliceResults := map[string][]interface{}{
		"key1": {"one", "two", "three", "four"},
		"key2": {"1", "2", "3", "4"},
		"key3": {"1.0", "1.1", "1.2", "1.3"},
	}

	for key, expected := range sliceResults {
		err = store.SetSlice(ctx, key, expected)
		is.NoError(err)

		expectedStrings, err := stringSlice(expected)
		is.NoError(err)

		v, err := store.GetSlice(ctx, key)
		is.NoError(err)
		strings, err := stringSlice(v)
		is.NoError(err)
		is.Equal(expectedStrings, strings)

		exists, err := store.Exists(ctx, key)
		is.NoError(err)
		is.True(exists)

		err = store.AppendSlice(ctx, key, "append1", "append2")
		is.NoError(err)

		v, err = store.GetSlice(ctx, key)
		is.NoError(err)

		expectedStrings = append(expectedStrings, []string{"append1", "append2"}...)
		sort.Strings(expectedStrings)
		values, err := stringSlice(v)
		is.NoError(err)
		is.Equal(expectedStrings, values)

		err = store.Delete(ctx, key)
		is.NoError(err)

		v, _ = store.GetSlice(ctx, key)
		is.Nil(v)

		exists, err = store.Exists(ctx, key)
		is.NoError(err)
		is.False(exists)

	}

	// Test set with duration
	expiration := 1
	err = store.SetWithExpiration(ctx, "foo", "bar", time.Duration(expiration)*time.Second)
	is.NoError(err)

	v, err := store.Get(ctx, "foo")
	is.NoError(err)
	val, ok := v.(string)
	is.True(ok)
	is.Equal("bar", val)

	time.Sleep(time.Duration(expiration) * time.Second)

	v, _ = store.Get(ctx, "foo")
	is.Nil(v)

	exists, err := store.Exists(ctx, "foo")
	is.NoError(err)
	is.False(exists)

}
