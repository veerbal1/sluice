package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	pool, err := pgxpool.New(ctx, "postgres://postgres:sluice@localhost:5432/sluices")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

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
		// 1. N message jama karo (100 tak, ya ctx cancel tak)
		msgs := make([]kafka.Message, 0, 100)
		for len(msgs) < 100 {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				break
			} // ctx cancel = shutdown
			msgs = append(msgs, m)
		}
		if len(msgs) == 0 {
			break
		}

		// 2. ek batch INSERT
		batch := &pgx.Batch{}
		for _, m := range msgs {
			id, _ := strconv.ParseInt(string(m.Value), 10, 64)
			batch.Queue("INSERT INTO events (id, acc) VALUES ($1,$2) ON CONFLICT DO NOTHING", id, string(m.Key))
		}
		br := pool.SendBatch(ctx, batch)
		if err := br.Close(); err != nil {
			log.Println("batch:", err)
		}

		count.Add(int64(len(msgs)))
		if n := count.Load(); n/10000 > (n-int64(len(msgs)))/10000 {
			fmt.Println(n, "  Total time:", time.Since(start))
			start = time.Now()
		}
	}
}
