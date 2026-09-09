package worker

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/client"
	"github.com/tkachyn/relay/internal/job"
	"github.com/tkachyn/relay/internal/server"
)

func TestExecuteCommand(t *testing.T) {
	result, err := execute(context.Background(), "echo relay")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(result), "relay") {
		t.Fatalf("got output %q", result)
	}
}

func TestWorkerExecutesAndReportsJob(t *testing.T) {
	testServer := httptest.NewServer(server.New().Handler())
	defer testServer.Close()

	apiClient := client.New(testServer.URL)
	created, err := apiClient.Submit(api.CreateJobRequest{
		Type:    "command",
		Payload: "echo relay",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- New("worker-1", testServer.URL, time.Millisecond).Run(ctx)
	}()

	for {
		current, err := apiClient.GetJob(created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == job.StatusCompleted {
			if current.Result != "relay" {
				t.Fatalf("got result %q", current.Result)
			}
			cancel()
			if err := <-runErr; err != nil {
				t.Fatal(err)
			}
			return
		}
		if current.Status == job.StatusFailed {
			t.Fatalf("job failed: %s", current.Error)
		}
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatal(err)
			}
			t.Fatal("worker stopped before completing the job")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
