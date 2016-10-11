package gokvstores

import (
	"container/list"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// CacheKVStore is the in-memory implementation of KVStore.
type CacheKVStore struct {
	mu         sync.Mutex
	expiration time.Duration
	maxEntries int
	ll         *list.List
	cache      map[string]*list.Element
}

// entry is the type stored in each *list.Element.
type entry struct {
	key   string
	value interface{}
}

// NewCacheKVStore returns in-memory KVStore.
func NewCacheKVStore(maxEntries int, expiration int) KVStore {
	return &CacheKVStore{
		maxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[string]*list.Element),
		expiration: time.Duration(expiration) * time.Second,
	}
}

func (c *CacheKVStore) Close() error {
	return nil
}

func (c *CacheKVStore) Flush() error {
	c.ll = list.New()
	c.cache = make(map[string]*list.Element)
	return nil
}

func (c *CacheKVStore) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.cache[key]; !ok {
		return fmt.Errorf("Key %s does not exist", key)
	}

	delete(c.cache, key)

	return nil
}

func (c *CacheKVStore) Exists(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.cache[key]

	return ok
}

func (c *CacheKVStore) Set(key string, value interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Already in cache?
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return nil
	}

	// Add to cache if not present
	ele := c.ll.PushFront(&entry{key, value})
	c.cache[key] = ele

	if c.ll.Len() > c.maxEntries && c.maxEntries != -1 {
		c.removeOldest()
	}

	return nil
}

func (c *CacheKVStore) Append(key string, value interface{}) error {
	var v string
	switch sv := value.(type) {
	case string:
		v = sv
	case []byte:
		v = string(sv)
	default:
		return fmt.Errorf("Value must be a string or []byte, got: %s", reflect.TypeOf(value))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	ee, ok := c.cache[key]
	if !ok {
		return fmt.Errorf("Key %s does not exist", key)
	}

	switch ev := ee.Value.(*entry).value.(type) {
	case string:
		ee.Value.(*entry).value = ev + v
	case []byte:
		ee.Value.(*entry).value = append(ev, v...)
	default:
		return fmt.Errorf("Can only append to a string or []byte, got: %s", reflect.TypeOf(value))
	}
	c.ll.MoveToFront(ee)

	if c.ll.Len() > c.maxEntries && c.maxEntries != -1 {
		c.removeOldest()
	}

	return nil
}

func (c *CacheKVStore) SetAdd(key string, value interface{}) error {
	svalue := fmt.Sprint(value)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Already in cache?
	if ee, ok := c.cache[key]; ok {
		// Assume the value is a map[string]bool
		m, ok := ee.Value.(*entry).value.(map[string]bool)
		if !ok {
			return fmt.Errorf("Key %s doesn't contain a set", key)
		}
		c.ll.MoveToFront(ee)
		m[svalue] = true
		return nil
	}

	// Add to cache if not present
	m := make(map[string]bool)
	m[svalue] = true
	ele := c.ll.PushFront(&entry{key, m})
	c.cache[key] = ele

	if c.ll.Len() > c.maxEntries && c.maxEntries != -1 {
		c.removeOldest()
	}

	return nil
}

func (c *CacheKVStore) SetMembers(key string) []interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		m, ok := ele.Value.(*entry).value.(map[string]bool)
		if !ok {
			return nil
		}

		i := 0
		result := make([]interface{}, len(m))
		for k, _ := range m {
			result[i] = k
			i++
		}

		c.ll.MoveToFront(ele)
		return result
	}

	return nil
}

// Get fetches the key's value from the cache.
// The ok result will be true if the item was found.
func (c *CacheKVStore) Get(key string) interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value
	}

	return nil
}

// RemoveOldest removes the oldest item in the cache and returns its key and value.
// If the cache is empty, the empty string and nil are returned.
func (c *CacheKVStore) RemoveOldest() (key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.removeOldest()
}

func (c *CacheKVStore) removeOldest() (key string, value interface{}) {
	ele := c.ll.Back()
	if ele == nil {
		return
	}

	c.ll.Remove(ele)

	ent := ele.Value.(*entry)

	delete(c.cache, ent.key)

	return ent.key, ent.value

}

// Len returns the number of items in the cache.
func (c *CacheKVStore) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.ll.Len()
}
