package server

import (
	"net/http/httptest"
	"testing"

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
