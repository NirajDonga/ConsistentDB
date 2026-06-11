package controller

import (
	"context"
	"errors"
	"kv_store/gen/kvpb"
	"kv_store/internal/ring"
	"kv_store/internal/storage"
)

type Server struct {
	kvpb.UnimplementedKVServer
	store     *storage.Store
	ring      *ring.Ring
	myAddress string
}

func NewServer(store *storage.Store, r *ring.Ring, myAddr string) *Server {
	return &Server{
		store:     store,
		ring:      r,
		myAddress: myAddr,
	}
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	ownerAddr, err := s.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	if ownerAddr != s.myAddress {
		return &kvpb.GetResponse{RedirectAddr: ownerAddr}, nil
	}

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
	ownerAddr, err := s.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	if ownerAddr != s.myAddress {
		return &kvpb.SetResponse{RedirectAddr: ownerAddr}, nil
	}

	s.store.Set(req.Key, req.Value)
	return &kvpb.SetResponse{Ok: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	ownerAddr, err := s.ring.GetNode(req.Key)
	if err != nil {
		return nil, err
	}

	if ownerAddr != s.myAddress {
		return &kvpb.DeleteResponse{RedirectAddr: ownerAddr}, nil
	}

	err = s.store.Delete(req.Key)

	if errors.Is(err, storage.ErrKeyNotFound) {
		return &kvpb.DeleteResponse{Deleted: false}, nil
	}

	if err != nil {
		return nil, err
	}

	return &kvpb.DeleteResponse{Deleted: true}, nil
}
