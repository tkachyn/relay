package queue

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/job"
)

func BenchmarkEnqueueClaimComplete(b *testing.B) {
	q := NewWithOptions(Options{RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second})
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := fmt.Sprintf("job-%d", index)
		current := job.New(id, "command", "echo benchmark", 0, time.Now().UTC())
		if err := q.Enqueue(current); err != nil {
			b.Fatal(err)
		}
		claimed, ok := q.Claim("worker-1")
		if !ok {
			b.Fatal("job was not claimable")
		}
		if _, err := q.Complete(claimed.ID, "worker-1", "ok"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentClaims(b *testing.B) {
	q := New()
	for index := 0; index < b.N; index++ {
		current := job.New(fmt.Sprintf("job-%d", index), "command", "echo benchmark", 0, time.Now().UTC())
		if err := q.Enqueue(current); err != nil {
			b.Fatal(err)
		}
	}
	var workerID atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := workerID.Add(1)
			claimed, ok := q.Claim(fmt.Sprintf("worker-%d", id))
			if !ok {
				b.Fatal("job was not claimable")
			}
			if _, err := q.Complete(claimed.ID, claimed.WorkerID, "ok"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkWorkerRecovery(b *testing.B) {
	q := NewWithOptions(Options{
		RetryBaseDelay: time.Nanosecond,
		RetryMaxDelay:  time.Nanosecond,
	})
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		current := job.New(fmt.Sprintf("recovery-%d", index), "command", "echo benchmark", 0, time.Now().UTC())
		current.MaxRetries = 1
		if err := q.Enqueue(current); err != nil {
			b.Fatal(err)
		}
		if _, ok := q.Claim("worker-lost"); !ok {
			b.Fatal("job was not claimable")
		}
		if recovered := q.RecoverWorker("worker-lost"); len(recovered) != 1 {
			b.Fatal("job was not recovered")
		}
	}
}
