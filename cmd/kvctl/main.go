package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"kv_store/gen/kvpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type App struct {
	client kvpb.KVClient
}

func main() {
	addr := "localhost:50051"

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	app := &App{
		client: kvpb.NewKVClient(conn),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt signal. Shutting down cleanly...")
		cancel()
	}()

	fmt.Println("Connected to KV Store! Type 'exit' to quit.")
	app.runREPL(ctx)
}

func (a *App) runREPL(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		if ctx.Err() != nil {
			return
		}

		fmt.Print("kv-store> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		args := strings.Fields(line)
		if len(args) == 0 {
			continue
		}

		command := strings.ToLower(args[0])

		switch command {
		case "get":
			a.handleGet(args)
		case "set":
			a.handleSet(args)
		case "delete":
			a.handleDelete(args)
		case "exit":
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Printf("unknown command: %s\n", command)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error reading standard input: %v\n", err)
	}
}

func (a *App) handleGet(args []string) {
	if len(args) != 2 {
		fmt.Println("usage: get <key>")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := a.client.Get(ctx, &kvpb.GetRequest{Key: args[1]})
	if err != nil {
		fmt.Printf("get failed: %v\n", err)
		return
	}
	if !res.Found {
		fmt.Println("key not found")
		return
	}
	fmt.Println(res.Value)
}

func (a *App) handleSet(args []string) {
	if len(args) != 3 {
		fmt.Println("usage: set <key> <value>")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := a.client.Set(ctx, &kvpb.SetRequest{Key: args[1], Value: args[2]})
	if err != nil {
		fmt.Printf("set failed: %v\n", err)
		return
	}
	fmt.Println("OK")
}

func (a *App) handleDelete(args []string) {
	if len(args) != 2 {
		fmt.Println("usage: delete <key>")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := a.client.Delete(ctx, &kvpb.DeleteRequest{Key: args[1]})
	if err != nil {
		fmt.Printf("delete failed: %v\n", err)
		return
	}
	if !res.Deleted {
		fmt.Println("key not found")
		return
	}
	fmt.Println("DELETED")
}
