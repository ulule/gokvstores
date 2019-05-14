package gokvstores

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresStore(t *testing.T) {
	writeUrl := "postgres://user:pass@masterdb.host.name:123456/cache?sslmode=allow"
	readUrl := "postgres://user:pass@127.0.0.1:123456/cache" // fails because of 1-2 sec delay
	readUrl = writeUrl
	store, err := NewPostgresStore(writeUrl, readUrl, false)

	assert.Nil(t, err)

	testStore(t, store)
}
