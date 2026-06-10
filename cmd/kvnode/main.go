package main

import (
	"kv_store/gen/kvpb"
	"kv_store/internal/controller"
	"kv_store/internal/storage"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {

	store := storage.NewStore()
	server := controller.NewServer(store)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	kvpb.RegisterKVServer(grpcServer, server)
	log.Println("kvnode listening on :50051")

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}
