package gokvstores

import (
	"testing"

	redis "gopkg.in/redis.v5"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	is := assert.New(t)

	con, err := NewRedisClientKVStore(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}, 30)

	is.Nil(err)
	defer con.Close()
	con.Flush()

	con.Set("key", "value")
	value, _ := String(con.Get("key"))
	is.Equal("value", value)
	is.True(con.Exists("key"))

	con.Delete("key")
	is.Nil(con.Get("key"))
	is.False(con.Exists("key"))

	// Sets
	con.SetAdd("myset", "hello")
	con.SetAdd("myset", "world")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world"}))

	con.SetAdd("myset", "hello")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world"}))

	con.SetAdd("myset", "hi")
	is.True(compareStringSets(con.SetMembers("myset"), []string{"hello", "world", "hi"}))

	con.Delete("myset")
	is.Nil(con.SetMembers("myset"))

	// Append
	con.Set("greetings", "Hello, ")

	con.Append("greetings", "World!")
	value, _ = String(con.Get("greetings"))
	assert.Equal(t, "Hello, World!", value)

	con.Append("greetings", " 123")
	value, _ = String(con.Get("greetings"))
	assert.Equal(t, "Hello, World! 123", value)
}

func compareStringSets(a []interface{}, b []string) bool {
	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	ma := make(map[string]bool)

	for _, aa := range a {
		s, err := String(aa)
		if err != nil {
			return false
		}
		ma[s] = true
	}

	for _, s := range b {
		if _, ok := ma[s]; !ok {
			return false
		}
	}

	return true
}
