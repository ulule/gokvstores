package gokvstores

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCache(t *testing.T) {
	kvstore := NewCacheKVStore(100)

	con := kvstore.Connection()
	defer con.Close()

	con.Set("key", "value")

	value, _ := String(con.Get("key"))

	assert.Equal(t, "value", value)

	assert.True(t, con.Exists("key"))

	con.Delete("key")

	assert.Equal(t, nil, con.Get("key"))

	assert.False(t, con.Exists("key"))
}
