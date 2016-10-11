package gokvstores

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	is := assert.New(t)

	con := NewCacheKVStore(100, 30)
	defer con.Close()

	// Set
	con.Set("key", "value")
	value, _ := String(con.Get("key"))
	is.Equal("value", value)
	is.True(con.Exists("key"))

	// Delete
	con.Delete("key")
	is.Nil(con.Get("key"))
	is.False(con.Exists("key"))

	// Append
	con.Set("greetings", "Hello, ")
	con.Append("greetings", "World!")

	value, _ = String(con.Get("greetings"))
	is.Equal("Hello, World!", value)

	con.Append("greetings", " 123")
	value, _ = String(con.Get("greetings"))
	is.Equal("Hello, World! 123", value)

	// SetAdd
	con.SetAdd("myset", "hello")
	con.SetAdd("myset", "world")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world"}))

	con.SetAdd("myset", "hello")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world"}))

	con.SetAdd("myset", "hi")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world", "hi"}))

	con.Delete("myset")
	is.Nil(con.SetMembers("myset"))
}
