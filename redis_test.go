package gokvstores

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRedis(t *testing.T) {
	kvstore := NewRedisKVStore("127.0.0.1", 6379, "", 0)

	con := kvstore.Connection()
	defer con.Close()

	con.Flush()

	con.Set("key", "value")

	value, _ := String(con.Get("key"))

	assert.Equal(t, "value", value)

	assert.True(t, con.Exists("key"))

	con.Delete("key")

	assert.Equal(t, nil, con.Get("key"))

	assert.False(t, con.Exists("key"))

	// Append
	con.Set("greetings", "Hello, ")

	con.Append("greetings", "World!")
	value, _ = String(con.Get("greetings"))
	assert.Equal(t, "Hello, World!", value)

	con.Append("greetings", " 123")
	value, _ = String(con.Get("greetings"))
	assert.Equal(t, "Hello, World! 123", value)
}
