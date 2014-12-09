package kvstores

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCache(t *testing.T) {
	kvstore := NewCacheKVStore(100)

	con := kvstore.Connection()
	defer con.Close()

	con.Set("key", "value")

	assert.Equal(t, "value", con.Get("key"))

	assert.True(t, con.Exists("key"))

	con.Delete("key")

	assert.Equal(t, "", con.Get("key"))

	assert.False(t, con.Exists("key"))
}
