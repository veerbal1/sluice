package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func CacheType() {
	os.Remove("temp.txt")
	file, err := os.OpenFile("temp.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	start := time.Now()

	for i := 0; i < 100000; i++ {
		_, err = file.WriteString(strconv.Itoa(i))
		if err != nil {
			log.Fatalf("फ़ाइल में लिखने में त्रुटि हुई: %s", err)
		}
	}

	total := time.Since(start)
	fmt.Printf("Cache: %v\n", total)
}

func SyncType() {
	os.Remove("temp.txt")
	file, err := os.OpenFile("temp.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	start := time.Now()

	for i := 0; i < 100000; i++ {
		_, err = file.WriteString(strconv.Itoa(i))
		file.Sync()
		if err != nil {
			log.Fatalf("फ़ाइल में लिखने में त्रुटि हुई: %s", err)
		}
	}

	total := time.Since(start)
	fmt.Printf("Sync: %v\n", total)
}

func BatchSync() {
	os.Remove("temp.txt")
	file, err := os.OpenFile("temp.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	start := time.Now()
	var buf bytes.Buffer
	for i := 0; i < 100000; i++ {
		buf.WriteString(strconv.Itoa(i))
	}
	_, err = file.Write(buf.Bytes())
	buf.Reset()
	file.Sync()
	if err != nil {
		log.Fatalf("फ़ाइल में लिखने में त्रुटि हुई: %s", err)
	}

	total := time.Since(start)
	fmt.Printf("Batch Sync: %v\n", total)
}

func BatchCache() {
	os.Remove("temp.txt")
	file, err := os.OpenFile("temp.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	start := time.Now()

	var buf bytes.Buffer
	for i := 0; i < 100000; i++ {
		buf.WriteString(strconv.Itoa(i))
	}
	_, err = file.Write(buf.Bytes())
	buf.Reset()
	// file.Sync()
	if err != nil {
		log.Fatalf("फ़ाइल में लिखने में त्रुटि हुई: %s", err)
	}

	total := time.Since(start)
	fmt.Printf("Batch Cache: %v\n", total)
}

func BigBatch(batchSize int, doSync bool) {
	os.Remove("temp.txt")
	file, err := os.OpenFile("temp.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer file.Close()

	start := time.Now()

	var buf bytes.Buffer
	pool := 1000 * 100
	iterations := int(pool / batchSize)

	for i := 0; i < iterations; i++ {
		for j := 0; j < batchSize; j++ {
			buf.WriteString(strconv.Itoa(j))
		}
		file.Write(buf.Bytes())
		buf.Reset()
		if doSync {
			file.Sync()
		}
	}

	total := time.Since(start)
	fmt.Printf("Batch Cache: %v: sync: %v\n", total, doSync)
}

func main() {
	// CacheType()
	// SyncType()
	// BatchSync()
	// BatchCache()
	BigBatch(1000, true)
	BigBatch(10000, true)
	BigBatch(100000, true)
}
