package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type request struct {
	Type     string `json:"type"`
	Payload  string `json:"payload"`
	Priority int    `json:"priority"`
}

func main() {
	serverURL := flag.String("server", "http://127.0.0.1:8080", "relay server URL")
	clients := flag.Int("clients", 10, "number of concurrent clients")
	requests := flag.Int("requests", 1000, "submissions per client")
	command := flag.String("command", "echo benchmark", "command payload")
	priority := flag.Int("priority", 0, "job priority")
	flag.Parse()
	if *clients <= 0 || *requests <= 0 {
		fatal(errors.New("clients and requests must be greater than zero"))
	}

	body, err := json.Marshal(request{
		Type:     "command",
		Payload:  *command,
		Priority: *priority,
	})
	if err != nil {
		fatal(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	started := time.Now()
	var completed atomic.Uint64
	var failed atomic.Uint64
	var latencyNanos atomic.Int64
	var waitGroup sync.WaitGroup
	waitGroup.Add(*clients)
	for clientIndex := 0; clientIndex < *clients; clientIndex++ {
		go func() {
			defer waitGroup.Done()
			for requestIndex := 0; requestIndex < *requests; requestIndex++ {
				start := time.Now()
				response, err := client.Post(*serverURL+"/v1/jobs", "application/json", bytes.NewReader(body))
				latencyNanos.Add(time.Since(start).Nanoseconds())
				if err != nil {
					failed.Add(1)
					continue
				}
				response.Body.Close()
				if response.StatusCode == http.StatusCreated {
					completed.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	waitGroup.Wait()

	total := completed.Load() + failed.Load()
	elapsed := time.Since(started)
	average := time.Duration(0)
	if total > 0 {
		average = time.Duration(latencyNanos.Load() / int64(total))
	}
	fmt.Printf("requests: %d\n", total)
	fmt.Printf("completed: %d\n", completed.Load())
	fmt.Printf("failed: %d\n", failed.Load())
	fmt.Printf("elapsed: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput: %.2f requests/s\n", float64(total)/elapsed.Seconds())
	fmt.Printf("average latency: %s\n", average.Round(time.Microsecond))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
