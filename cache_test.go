package gokvstores

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	store, err := NewCacheKVStore(time.Second*10, time.Second*10)
	assert.Nil(t, err)

	testStore(t, store)
}
