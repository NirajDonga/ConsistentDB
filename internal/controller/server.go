package controller

import (
	"context"
	"errors"
	"kv_store/gen/kvpb"
	"kv_store/internal/storage"
)

type Server struct {
	kvpb.UnimplementedKVServer
	store *storage.Store
}

func NewServer(store *storage.Store) *Server {
	return &Server{
		store: store,
	}
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, err := s.store.Get(req.Key)

	if errors.Is(err, storage.ErrKeyNotFound) {
		return &kvpb.GetResponse{Found: false}, nil
	}

	if err != nil {
		return nil, err
	}

	return &kvpb.GetResponse{
		Value: value,
		Found: true,
	}, nil
}

func (s *Server) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	s.store.Set(req.Key, req.Value)
	return &kvpb.SetResponse{Ok: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	err := s.store.Delete(req.Key)

	if errors.Is(err, storage.ErrKeyNotFound) {
		return &kvpb.DeleteResponse{Deleted: false}, nil
	}

	if err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{Deleted: true}, nil
}
