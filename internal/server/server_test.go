package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/client"
	"github.com/tkachyn/relay/internal/job"
)

func TestJobLifecycleThroughHTTP(t *testing.T) {
	testServer := httptest.NewServer(New().Handler())
	defer testServer.Close()
	apiClient := client.New(testServer.URL)

	if _, err := apiClient.RegisterWorker("worker-1"); err != nil {
		t.Fatal(err)
	}
	created, err := apiClient.Submit(api.CreateJobRequest{
		Type:     "command",
		Payload:  "echo relay",
		Priority: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != job.StatusQueued {
		t.Fatalf("got status %s, expected %s", created.Status, job.StatusQueued)
	}

	claimed, found, err := apiClient.ClaimJob("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected worker to claim a job")
	}
	if claimed.ID != created.ID || claimed.Status != job.StatusRunning {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}

	completed, err := apiClient.ReportResult(created.ID, api.ResultRequest{
		WorkerID: "worker-1",
		Success:  true,
		Result:   "relay",
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != job.StatusCompleted || completed.Result != "relay" {
		t.Fatalf("unexpected completed job: %+v", completed)
	}

	current, err := apiClient.GetJob(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != job.StatusCompleted {
		t.Fatalf("got status %s, expected %s", current.Status, job.StatusCompleted)
	}
	history, err := apiClient.GetHistory(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("got history length %d, expected 3", len(history))
	}
	stats, err := apiClient.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Queue.Completed != 1 || stats.Workers.Healthy != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestClaimReturnsEmptyWhenNoJobsAreAvailable(t *testing.T) {
	testServer := httptest.NewServer(New().Handler())
	defer testServer.Close()
	apiClient := client.New(testServer.URL)

	if _, err := apiClient.RegisterWorker("worker-1"); err != nil {
		t.Fatal(err)
	}
	_, found, err := apiClient.ClaimJob("worker-1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("found a job in an empty queue")
	}
}

func TestJobSubmissionRejectsInvalidJSONAndTimeouts(t *testing.T) {
	testServer := httptest.NewServer(New().Handler())
	defer testServer.Close()

	tests := []struct {
		name       string
		body       string
		statusCode int
	}{
		{
			name:       "trailing json",
			body:       `{"payload":"echo ok"} {"payload":"echo again"}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "negative timeout",
			body:       `{"payload":"echo ok","timeout":"-1s"}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "zero timeout",
			body:       `{"payload":"echo ok","timeout":"0s"}`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "oversized body",
			body:       `{"payload":"` + strings.Repeat("x", 1<<20) + `"}`,
			statusCode: http.StatusRequestEntityTooLarge,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := testServer.Client().Post(
				testServer.URL+"/v1/jobs",
				"application/json",
				strings.NewReader(test.body),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.statusCode {
				t.Fatalf("got status %d, expected %d", response.StatusCode, test.statusCode)
			}
		})
	}
}

func TestServerRestoresJobsAndWorkers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay-state.json")
	first, err := NewWithOptions(Options{StoragePath: path})
	if err != nil {
		t.Fatal(err)
	}
	firstServer := httptest.NewServer(first.Handler())
	firstClient := client.New(firstServer.URL)

	if _, err := firstClient.RegisterWorker("worker-1"); err != nil {
		t.Fatal(err)
	}
	created, err := firstClient.Submit(api.CreateJobRequest{
		Type:       "command",
		Payload:    "echo recover",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := firstClient.ClaimJob("worker-1"); err != nil || !found {
		t.Fatalf("claim failed: found=%v err=%v", found, err)
	}
	firstServer.Close()

	second, err := NewWithOptions(Options{StoragePath: path})
	if err != nil {
		t.Fatal(err)
	}
	secondServer := httptest.NewServer(second.Handler())
	defer secondServer.Close()
	secondClient := client.New(secondServer.URL)

	restored, err := secondClient.GetJob(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Status != job.StatusQueued || restored.WorkerID != "" {
		t.Fatalf("unexpected restored job: %+v", restored)
	}
	workers, err := secondClient.ListWorkers()
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].Status != "dead" {
		t.Fatalf("unexpected restored workers: %+v", workers)
	}
}

func TestServerRecoversJobsFromDeadWorker(t *testing.T) {
	testServer := NewWithOptionsOrFail(t, Options{
		HeartbeatTimeout: 15 * time.Millisecond,
		MonitorInterval:  5 * time.Millisecond,
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    time.Millisecond,
	})
	httpServer := httptest.NewServer(testServer.Handler())
	defer httpServer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testServer.Start(ctx)
	apiClient := client.New(httpServer.URL)

	if _, err := apiClient.RegisterWorker("worker-1"); err != nil {
		t.Fatal(err)
	}
	created, err := apiClient.Submit(api.CreateJobRequest{
		Type:       "command",
		Payload:    "echo recover",
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := apiClient.ClaimJob("worker-1"); err != nil || !found {
		t.Fatalf("claim failed: found=%v err=%v", found, err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err := apiClient.GetJob(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		workers, err := apiClient.ListWorkers()
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == job.StatusQueued && len(workers) == 1 && workers[0].Status == "dead" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("worker failure was not recovered")
}

func TestOperationalEndpoints(t *testing.T) {
	testServer := httptest.NewServer(New().Handler())
	defer testServer.Close()

	for _, path := range []string{"/dashboard/", "/metrics", "/v1/stats"} {
		response, err := testServer.Client().Get(testServer.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, response.StatusCode, body)
		}
		if path == "/dashboard/" && !strings.Contains(string(body), "Relay Dashboard") {
			t.Fatal("dashboard response did not contain its title")
		}
		if path == "/metrics" && !strings.Contains(string(body), "relay_queue_depth") {
			t.Fatal("metrics response did not contain queue depth")
		}
	}
}

func NewWithOptionsOrFail(t *testing.T, options Options) *Server {
	t.Helper()
	server, err := NewWithOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	return server
}
