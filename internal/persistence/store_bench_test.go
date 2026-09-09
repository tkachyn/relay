package persistence

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/job"
)

func BenchmarkStateSave(b *testing.B) {
	store := New(filepath.Join(b.TempDir(), "relay-state.json"))
	state := State{
		Jobs: make([]*job.Job, 100),
	}
	for index := range state.Jobs {
		state.Jobs[index] = job.New(
			fmt.Sprintf("job-%d", index),
			"command",
			"echo benchmark",
			index%5,
			time.Now().UTC(),
		)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Save(state); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateLoad(b *testing.B) {
	store := New(filepath.Join(b.TempDir(), "relay-state.json"))
	state := State{
		Jobs: make([]*job.Job, 100),
	}
	for index := range state.Jobs {
		state.Jobs[index] = job.New(
			fmt.Sprintf("job-%d", index),
			"command",
			"echo benchmark",
			index%5,
			time.Now().UTC(),
		)
	}
	if err := store.Save(state); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := store.Load(); err != nil {
			b.Fatal(err)
		}
	}
}
