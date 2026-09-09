package persistence

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/job"
)

func TestStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay-state.json")
	store := New(path)
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	state := State{
		Jobs: []*job.Job{
			job.New("job-1", "command", "echo relay", 3, createdAt),
		},
		Workers: []api.Worker{
			{
				ID:           "worker-1",
				RegisteredAt: createdAt,
				LastSeen:     createdAt,
				Status:       "healthy",
			},
		},
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Jobs) != 1 || loaded.Jobs[0].ID != "job-1" {
		t.Fatalf("unexpected jobs: %+v", loaded.Jobs)
	}
	if len(loaded.Workers) != 1 || loaded.Workers[0].Status != "healthy" {
		t.Fatalf("unexpected workers: %+v", loaded.Workers)
	}
}
