package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "events",
		GroupID:        "workers",
		CommitInterval: time.Second,
	})
	defer r.Close()
	f, err := os.OpenFile("seen.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var count atomic.Int64
	start := time.Now()
	for {
		message, err := r.ReadMessage(ctx) // ek message, ruk kar
		if err != nil {
			break
		}
		count.Add(1)
		fmt.Fprintln(f, string(message.Value))

		num := count.Load()

		if num == 1 {
			fmt.Println("First", string(message.Value))
		}

		if (num % 10000) == 0 {
			fmt.Println(string(message.Value))
			totalTime := time.Since(start)
			fmt.Println("Total time: ", totalTime)
			start = time.Now()
		}
		time.Sleep(1 * time.Millisecond)
	}
}
