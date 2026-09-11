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

func TestFailRetriesWithBackoffUntilLimit(t *testing.T) {
	q := NewWithOptions(Options{
		RetryBaseDelay: 2 * time.Millisecond,
		RetryMaxDelay:  8 * time.Millisecond,
	})
	retryJob := job.New("retry", "command", "echo retry", 0, time.Now().UTC())
	retryJob.MaxRetries = 2
	if err := q.Enqueue(retryJob); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		current, ok := q.Claim("worker-1")
		if !ok {
			t.Fatalf("expected attempt %d", attempt)
		}
		if current.Attempts != attempt {
			t.Fatalf("got attempt %d, expected %d", current.Attempts, attempt)
		}
		failed, err := q.Fail(current.ID, "worker-1", "", "failed")
		if err != nil {
			t.Fatal(err)
		}
		if attempt < 3 {
			if failed.Status != job.StatusQueued || failed.NextAttemptAt == nil {
				t.Fatalf("expected retry state: %+v", failed)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if failed.Status != job.StatusFailed || failed.NextAttemptAt != nil {
			t.Fatalf("expected terminal failure: %+v", failed)
		}
	}
}

func TestExpireRetriesTimedOutJob(t *testing.T) {
	q := NewWithOptions(Options{RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond})
	timeoutJob := job.New("timeout", "command", "sleep", 0, time.Now().UTC())
	timeoutJob.Timeout = "1ms"
	if err := q.Enqueue(timeoutJob); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Claim("worker-1"); !ok {
		t.Fatal("expected job to be claimed")
	}
	time.Sleep(5 * time.Millisecond)
	expired := q.Expire(time.Now().UTC())
	if len(expired) != 1 || expired[0].Status != job.StatusFailed {
		t.Fatalf("unexpected expired jobs: %+v", expired)
	}
}

func TestRestoreRequeuesRunningJobs(t *testing.T) {
	q := New()
	running := job.New("running", "command", "echo running", 0, time.Now().UTC())
	running.Status = job.StatusRunning
	running.WorkerID = "worker-1"
	if err := q.Restore([]*job.Job{running}); err != nil {
		t.Fatal(err)
	}
	current, ok := q.Claim("worker-2")
	if !ok {
		t.Fatal("expected restored job to be queued")
	}
	if current.Status != job.StatusRunning || current.WorkerID != "worker-2" {
		t.Fatalf("unexpected restored job: %+v", current)
	}
}

func TestDelayedJobIsUnavailableUntilScheduledTime(t *testing.T) {
	q := New()
	scheduledAt := time.Now().UTC().Add(40 * time.Millisecond)
	delayed := job.New("delayed", "command", "echo delayed", 10, time.Now().UTC())
	delayed.ScheduledAt = &scheduledAt
	if err := q.Enqueue(delayed); err != nil {
		t.Fatal(err)
	}
	if _, ok := q.Claim("worker-1"); ok {
		t.Fatal("claimed a delayed job before its schedule")
	}
	time.Sleep(50 * time.Millisecond)
	current, ok := q.Claim("worker-1")
	if !ok || current.ID != "delayed" {
		t.Fatalf("expected delayed job after schedule, got %v", current)
	}
}

func TestFIFOPolicyIgnoresPriority(t *testing.T) {
	q := NewWithOptions(Options{Policy: PolicyFIFO})
	createdAt := time.Now().UTC()
	early := job.New("early", "command", "echo early", 1, createdAt)
	late := job.New("late", "command", "echo late", 10, createdAt.Add(time.Millisecond))
	if err := q.Enqueue(early); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(late); err != nil {
		t.Fatal(err)
	}
	current, ok := q.Claim("worker-1")
	if !ok || current.ID != "early" {
		t.Fatalf("expected FIFO job early, got %v", current)
	}
}

func TestJobHistoryRecordsTransitions(t *testing.T) {
	q := New()
	current := job.New("history", "command", "echo history", 0, time.Now().UTC())
	if err := q.Enqueue(current); err != nil {
		t.Fatal(err)
	}
	claimed, ok := q.Claim("worker-1")
	if !ok {
		t.Fatal("expected job to be claimed")
	}
	if _, err := q.Complete(claimed.ID, "worker-1", "history"); err != nil {
		t.Fatal(err)
	}
	finished, err := q.Get("history")
	if err != nil {
		t.Fatal(err)
	}
	if len(finished.History) != 3 {
		t.Fatalf("got history length %d: %+v", len(finished.History), finished.History)
	}
	for index, eventType := range []string{"created", "claimed", "completed"} {
		if finished.History[index].Type != eventType {
			t.Fatalf("event %d was %q, expected %q", index, finished.History[index].Type, eventType)
		}
	}
}

func TestRetryDelayKeepsConfiguredBaseWhenMaxIsSmaller(t *testing.T) {
	q := NewWithOptions(Options{
		RetryBaseDelay: 2 * time.Minute,
		RetryMaxDelay:  time.Minute,
	})
	if got := q.retryDelay(1); got != 2*time.Minute {
		t.Fatalf("got retry delay %s, expected 2m", got)
	}
}
