package gokvstores

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresStore(t *testing.T) {
	writeUrl := "postgres://user:pass@masterdb.host.name:123456/cache?sslmode=allow"
	readUrl := "postgres://user:pass@127.0.0.1:123456/cache" // fails because of 1-2 sec delay
	readUrl = writeUrl
	store, err := NewPostgresStore(writeUrl, readUrl)
	assert.Nil(t, err)

	err = CreateSchema(store)
	if err != nil {
		fmt.Println(err)
	}

	testStore(t, store)
}
