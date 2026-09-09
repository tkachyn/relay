package queue

import (
	"testing"
	"time"

	"github.com/tkachyn/relay/internal/job"
)

func TestClaimUsesPriorityAndFIFOOrder(t *testing.T) {
	q := New()
	createdAt := time.Now().UTC()
	jobs := []*job.Job{
		job.New("low", "command", "low", 1, createdAt),
		job.New("high-first", "command", "high-first", 10, createdAt.Add(time.Millisecond)),
		job.New("high-second", "command", "high-second", 10, createdAt.Add(2*time.Millisecond)),
	}
	for _, current := range jobs {
		if err := q.Enqueue(current); err != nil {
			t.Fatalf("enqueue %s: %v", current.ID, err)
		}
	}

	for _, expectedID := range []string{"high-first", "high-second", "low"} {
		current, ok := q.Claim("worker-1")
		if !ok {
			t.Fatalf("expected job %s", expectedID)
		}
		if current.ID != expectedID {
			t.Fatalf("claimed %s, expected %s", current.ID, expectedID)
		}
	}
	if _, ok := q.Claim("worker-1"); ok {
		t.Fatal("claimed a job from an empty queue")
	}
}

func TestCompleteRequiresOwningWorker(t *testing.T) {
	q := New()
	if err := q.Enqueue(job.New("job-1", "command", "echo ok", 0, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Claim("worker-1"); !ok {
		t.Fatal("expected job to be claimed")
	}
	if _, err := q.Complete("job-1", "worker-2", "ok"); err != ErrInvalidTransition {
		t.Fatalf("got %v, expected %v", err, ErrInvalidTransition)
	}
	completed, err := q.Complete("job-1", "worker-1", "ok")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != job.StatusCompleted || completed.Result != "ok" {
		t.Fatalf("unexpected completed job: %+v", completed)
	}
}

func TestCancelRemovesQueuedJobFromClaims(t *testing.T) {
	q := New()
	if err := q.Enqueue(job.New("job-1", "command", "echo ok", 0, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Cancel("job-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Claim("worker-1"); ok {
		t.Fatal("claimed a cancelled job")
	}
	current, err := q.Get("job-1")
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != job.StatusCancelled {
		t.Fatalf("got status %s, expected %s", current.Status, job.StatusCancelled)
	}
}
