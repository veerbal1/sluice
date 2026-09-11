package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func drain(ch <-chan kafka.Message, max int) []kafka.Message {
	msgs := make([]kafka.Message, 0, max)
	msgs = append(msgs, <-ch)
	for len(msgs) < max {
		select {
		case m := <-ch:
			msgs = append(msgs, m)
		default:
			return msgs
		}
	}
	return msgs
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	channel := make(chan kafka.Message, 4096)
	kafkaW := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "events",
		Balancer:     &kafka.Hash{},
		BatchTimeout: time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaW.Close()
	var id atomic.Int64

	var lock sync.Mutex
	num := 0
	numberOfWorkers := 150
	var dropped atomic.Int64

	for i := 0; i < numberOfWorkers; i++ {
		go func() {
			for {
				msgs := drain(channel, 100)

				err := kafkaW.WriteMessages(ctx, msgs...)
				if err != nil {
					fmt.Println("Error: ", err)
				}

				lock.Lock()
				num += len(msgs)
				lock.Unlock()
			}
		}()
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		select {
		case channel <- kafka.Message{
			Key:   []byte(r.URL.Query().Get("acc")),
			Value: []byte(strconv.FormatInt(id.Add(1), 10)),
		}:
		default:
			dropped.Add(1)
			http.Error(w, "buffer full", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ok")
		d := time.Since(start)

		if d > 500*time.Microsecond {
			log.Println("slow:", d)
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		numStr := strconv.Itoa(num)
		lock.Unlock()
		dropppedStr := strconv.Itoa(int(dropped.Load()))
		fmt.Fprintln(w, numStr+" - "+dropppedStr)
	})

	fmt.Println("Listening on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
