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
	value, ok := s.store.Get(req.Key)

	if !ok {
		return nil, errors.New("Key Not Found")
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
	ok := s.store.Delete(req.Key)

	if !ok {
		return nil, errors.New("Server error")
	}

	return &kvpb.DeleteResponse{Deleted: true}, nil
}
