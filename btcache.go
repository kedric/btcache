package btcache

import (
	"time"

	"github.com/google/btree"
)

type DeleteEvent uint

const (
	DeleteEventReplaced DeleteEvent = iota
	DeleteEventLifeTime
	DeleteEventMaxItem
	DeleteEventDeleted
)

type CacheKey[K any] interface {
	Less(K) bool
}

type CacheConfig[K CacheKey[K], V any] struct {
	OnDelete func(DeleteEvent, K, V)
	LifeTime time.Duration
	Windows  time.Duration
	MaxItem  int
}

type cacheItem[K CacheKey[K], V any] struct {
	Key           K
	Value         V
	LastTimeTouch time.Time
}

func (left *cacheItem[K, V]) Less(right *cacheItem[K, V]) bool {
	return left.Key.Less(right.Key)
}

type Cache[K CacheKey[K], V any] struct {
	tree   *btree.BTreeG[*cacheItem[K, V]]
	config CacheConfig[K, V]
}

func Less[K CacheKey[K]](left K, right K) bool {
	return left.Less(right)
}

func NewCache[K CacheKey[K], V any](degree int, config CacheConfig[K, V]) *Cache[K, V] {
	return &Cache[K, V]{
		tree:   btree.NewG[*cacheItem[K, V]](degree, Less[*cacheItem[K, V]]),
		config: config,
	}
}

func (impl *Cache[K, V]) Get(key K) V {
	tmp := &cacheItem[K, V]{
		Key: key,
	}
	if v, ok := impl.tree.Get(tmp); ok {
		v.LastTimeTouch = time.Now()
		return v.Value
	}
	var noop V
	return noop
}

func (impl *Cache[K, V]) Set(key K, value V) {
	tmp := &cacheItem[K, V]{
		Key:           key,
		Value:         value,
		LastTimeTouch: time.Now(),
	}
	if impl.config.MaxItem > 0 {
		if impl.tree.Len() >= impl.config.MaxItem && !impl.tree.Has(tmp) {
			impl.deleteOlder()
		}
	}
	if v, ok := impl.tree.ReplaceOrInsert(tmp); ok {
		impl.config.OnDelete(DeleteEventReplaced, v.Key, v.Value)
	}
}

func (impl *Cache[K, V]) Delete(key K) {
	tmp := &cacheItem[K, V]{
		Key: key,
	}
	if v, ok := impl.tree.Delete(tmp); ok {
		impl.config.OnDelete(DeleteEventDeleted, v.Key, v.Value)
	}
}

func (impl *Cache[K, V]) StartBackgroundCleanUp() {
	if impl.config.Windows <= 0 {
		impl.config.Windows = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(impl.config.Windows)
		for {
			select {
			case <-ticker.C:
				impl.CleanUp()
			}
		}
	}()
}

func (impl *Cache[K, V]) deleteOlder() {
	var toDelete *cacheItem[K, V]
	impl.tree.Descend(func(item *cacheItem[K, V]) bool {
		if toDelete == nil {
			toDelete = item
			return true
		}
		if item.LastTimeTouch.Before(toDelete.LastTimeTouch) {
			toDelete = item
		}
		return true
	})
	impl.tree.Delete(toDelete)
	impl.config.OnDelete(DeleteEventMaxItem, toDelete.Key, toDelete.Value)
}

func (impl *Cache[K, V]) Clear() {
	impl.tree.Clear(false)
}

func (impl *Cache[K, V]) CleanUp() {
	impl.tree.Descend(func(item *cacheItem[K, V]) bool {
		if time.Since(item.LastTimeTouch) > impl.config.LifeTime {
			impl.tree.Delete(item)
			impl.config.OnDelete(DeleteEventLifeTime, item.Key, item.Value)
		}
		return true
	})
}
