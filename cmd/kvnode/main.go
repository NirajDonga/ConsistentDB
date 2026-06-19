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

	log.Println("Replaying Write-Ahead Log...")
	if err := storage.Replay("data.wal", store); err != nil {
		log.Fatalf("Failed to replay WAL: %v", err)
	}
	log.Println("WAL replay complete!")

	wal, err := storage.NewWal("data.wal")
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	server := controller.NewServer(store, wal)

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
