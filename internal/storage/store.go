package storage

import (
	"hash/fnv"
	"sync"
	"time"
)

const shardCount = 32
const shardMaxSize = 1000
const defaultTTLSeconds = 60

type Node struct {
	key       string
	value     string
	expiresAt int64
	prev      *Node
	next      *Node
}

type Shard struct {
	data     map[string]*Node
	mu       sync.RWMutex
	head     *Node
	tail     *Node
	capacity int
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
			data:     make(map[string]*Node),
			capacity: shardMaxSize,
		}
	}
	return store
}

func (s *Store) getShard(key string) *Shard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	shardIndex := hasher.Sum32() % uint32(shardCount)
	return s.shards[shardIndex]
}

func (s *Store) Get(key string) (string, bool) {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	node, ok := shard.data[key]
	if ok {
		if time.Now().Unix() > node.expiresAt {
			shard.removeNode(node)
			delete(shard.data, key)
			return "", false
		}
		shard.moveToFront(node)
		return node.value, true
	}
	return "", false
}

func (s *Store) Set(key string, value string) {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	node, ok := shard.data[key]
	if ok {
		node.value = value
		shard.moveToFront(node)
		return
	}

	if shard.capacity == len(shard.data) {
		delete(shard.data, shard.tail.key)
		shard.removeNode(shard.tail)
	}

	node = &Node{
		key:       key,
		value:     value,
		expiresAt: time.Now().Unix() + defaultTTLSeconds,
	}

	shard.addNode(node)
	shard.data[key] = node
}

func (s *Store) Delete(key string) bool {

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if node, exists := shard.data[key]; exists {
		shard.removeNode(node)
		delete(shard.data, key)
		return true
	}
	return false
}

func (s *Shard) addNode(node *Node) {
	if s.head == nil {
		s.head = node
		s.tail = node
	} else {
		s.head.next = node
		node.prev = s.head
		s.head = node
	}
}

func (s *Shard) removeNode(node *Node) {
	if s.head == s.tail {
		s.head = nil
		s.tail = nil
	} else if s.head == node {
		s.head = node.prev
		s.head.next = nil
	} else if s.tail == node {
		s.tail = s.tail.next
		s.tail.prev = nil
	} else {
		node.prev.next = node.next
		node.next.prev = node.prev
	}
}

func (s *Shard) moveToFront(node *Node) {
	if s.head == node {
		return
	}
	s.removeNode(node)
	s.addNode(node)
}
