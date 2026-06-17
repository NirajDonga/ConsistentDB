package storage

import (
	"errors"
	"hash/fnv"
	"sync"
)

const shardCount = 32

type Shard struct {
	data map[string]string
	mu   sync.RWMutex
}

type Store struct {
	shards []*Shard
}

func NewStore() *Store {
	store := &Store{
		shards: make([]*Shard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		store.shards[i] = &Shard{
			data: make(map[string]string),
		}
	}
	return store
}

func getShardIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % shardCount
}
func (s *Store) getShard(key string) *Shard {
	return s.shards[getShardIndex(key)]
}

var ErrKeyNotFound = errors.New("key not found")

func (s *Store) Get(key string) (string, error) {
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	value, ok := shard.data[key]
	if ok {
		return value, nil
	}
	return "", ErrKeyNotFound
}

func (s *Store) Set(key string, value string) {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = value
}

func (s *Store) Delete(key string) error {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.data[key]; !exists {
		return ErrKeyNotFound
	}
	delete(shard.data, key)
	return nil
}
