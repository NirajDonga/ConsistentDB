package ring

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type VirtualNode struct {
	Hash uint64
	Node string
}

type Ring struct {
	nodes    map[string]struct{}
	vNodes   []VirtualNode
	replicas int
	mu       sync.RWMutex
}

var ErrEmptyRing = errors.New("consistent hash ring is empty")
var ErrInvalidReplicas = errors.New("replicas must be greater than 0")

func NewRing(replicas int) (*Ring, error) {
	if replicas <= 0 {
		return nil, ErrInvalidReplicas
	}
	return &Ring{
		nodes:    make(map[string]struct{}),
		vNodes:   make([]VirtualNode, 0),
		replicas: replicas,
	}, nil
}

func hashFNV64(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (r *Ring) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node]; exists {
		return
	}
	r.nodes[node] = struct{}{}

	var buf []byte
	base := node + "#"

	for i := 0; i < r.replicas; i++ {
		buf = append(buf[:0], base...)
		buf = strconv.AppendInt(buf, int64(i), 10)
		hash := hashFNV64(string(buf))

		vNode := VirtualNode{Hash: hash, Node: node}

		idx := sort.Search(len(r.vNodes), func(j int) bool {
			return r.vNodes[j].Hash >= hash
		})
		r.vNodes = append(r.vNodes, VirtualNode{})

		copy(r.vNodes[idx+1:], r.vNodes[idx:])

		r.vNodes[idx] = vNode
	}
}

func (r *Ring) GetNode(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vNodes) == 0 {
		return "", ErrEmptyRing
	}

	hash := hashFNV64(key)
	idx := sort.Search(len(r.vNodes), func(i int) bool {
		return r.vNodes[i].Hash >= hash
	})

	if idx == len(r.vNodes) {
		idx = 0
	}

	return r.vNodes[idx].Node, nil
}

func (r *Ring) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node]; !exists {
		return
	}

	delete(r.nodes, node)

	w := 0
	for _, vn := range r.vNodes {
		if vn.Node != node {
			r.vNodes[w] = vn
			w++
		}
	}

	for i := w; i < len(r.vNodes); i++ {
		r.vNodes[i] = VirtualNode{}
	}
	r.vNodes = r.vNodes[:w]

}
