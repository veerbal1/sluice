package main

import (
	"fmt"
	"net/http"
	"strconv"
)

func main() {
	num := 0
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		num++
		fmt.Fprintf(w, "ok")
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		num++
		numStr := strconv.Itoa(num)
		fmt.Fprintf(w, "%s", numStr)
	})
}
