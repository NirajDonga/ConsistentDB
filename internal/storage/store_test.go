package storage

import (
	"strconv"
	"testing"
)

func BenchmarkStoreWritesConcurrent(b *testing.B) {
	store := NewStore()
	b.ResetTimer()

	// b.RunParallel spins up multiple goroutines (one per CPU core)
	// to run this loop at the exact same time.
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Each core writes different keys, spreading the load across all 32 shards!
			store.Set(strconv.Itoa(i), "value")
			i++
		}
	})
}

func BenchmarkStoreReadsConcurrent(b *testing.B) {
	store := NewStore()

	// Pre-fill 1000 keys so we have data scattered across our 32 shards
	for i := 0; i < 1000; i++ {
		store.Set(strconv.Itoa(i), "value")
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Read different keys to ensure we are hitting different shards concurrently
			store.Get(strconv.Itoa(i % 1000))
			i++
		}
	})
}
