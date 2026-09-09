package queue

import (
	"container/heap"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/tkachyn/relay/internal/job"
)

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrInvalidTransition = errors.New("invalid job transition")
)

type Queue struct {
	mu             sync.Mutex
	jobs           map[string]*job.Job
	queued         jobHeap
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration
}

type Options struct {
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

func New() *Queue {
	return NewWithOptions(Options{
		RetryBaseDelay: time.Second,
		RetryMaxDelay:  time.Minute,
	})
}

func NewWithOptions(options Options) *Queue {
	if options.RetryBaseDelay <= 0 {
		options.RetryBaseDelay = time.Second
	}
	if options.RetryMaxDelay < options.RetryBaseDelay {
		options.RetryMaxDelay = time.Minute
	}
	return &Queue{
		jobs:           make(map[string]*job.Job),
		retryBaseDelay: options.RetryBaseDelay,
		retryMaxDelay:  options.RetryMaxDelay,
	}
}

func (q *Queue) Enqueue(newJob *job.Job) error {
	if newJob == nil || newJob.ID == "" {
		return errors.New("job must have an id")
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if _, exists := q.jobs[newJob.ID]; exists {
		return errors.New("job already exists")
	}

	stored := newJob.Clone()
	stored.Status = job.StatusQueued
	q.jobs[stored.ID] = stored
	q.pushLocked(stored)
	return nil
}

func (q *Queue) Restore(jobs []*job.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = make(map[string]*job.Job, len(jobs))
	q.queued = nil
	for _, current := range jobs {
		if current == nil || current.ID == "" {
			return errors.New("restored job must have an id")
		}
		stored := current.Clone()
		if stored.Status == job.StatusRunning {
			stored.Status = job.StatusQueued
			stored.StartedAt = nil
			stored.WorkerID = ""
		}
		q.jobs[stored.ID] = stored
		if stored.Status == job.StatusQueued {
			q.pushLocked(stored)
		}
	}
	return nil
}

func (q *Queue) Claim(workerID string) (*job.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().UTC()
	bestIndex := -1
	for index, item := range q.queued {
		stored, exists := q.jobs[item.jobID]
		if !exists || stored.Status != job.StatusQueued {
			continue
		}
		if stored.NextAttemptAt != nil && stored.NextAttemptAt.After(now) {
			continue
		}
		if bestIndex == -1 || q.queued.Less(index, bestIndex) {
			bestIndex = index
		}
	}
	if bestIndex == -1 {
		return nil, false
	}

	item := heap.Remove(&q.queued, bestIndex).(*queueItem)
	stored := q.jobs[item.jobID]
	stored.Status = job.StatusRunning
	stored.StartedAt = &now
	stored.Attempts++
	stored.WorkerID = workerID
	stored.NextAttemptAt = nil
	return stored.Clone(), true
}

func (q *Queue) Fail(id, workerID, result, failure string) (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, exists := q.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}
	if stored.Status != job.StatusRunning || stored.WorkerID != workerID {
		return nil, ErrInvalidTransition
	}
	q.failLocked(stored, result, failure, time.Now().UTC())
	return stored.Clone(), nil
}

func (q *Queue) Complete(id, workerID, result string) (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, exists := q.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}
	if stored.Status != job.StatusRunning || stored.WorkerID != workerID {
		return nil, ErrInvalidTransition
	}

	now := time.Now().UTC()
	stored.Status = job.StatusCompleted
	stored.CompletedAt = &now
	stored.NextAttemptAt = nil
	stored.Result = result
	stored.Error = ""
	return stored.Clone(), nil
}

func (q *Queue) Expire(now time.Time) []*job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	var expired []*job.Job
	for _, stored := range q.jobs {
		if stored.Status != job.StatusRunning || stored.StartedAt == nil || stored.Timeout == "" {
			continue
		}
		timeout, err := time.ParseDuration(stored.Timeout)
		if err != nil || now.Before(stored.StartedAt.Add(timeout)) {
			continue
		}
		q.failLocked(stored, stored.Result, "job timed out", now)
		expired = append(expired, stored.Clone())
	}
	return expired
}

func (q *Queue) RecoverWorker(workerID string) []*job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now().UTC()
	var recovered []*job.Job
	for _, stored := range q.jobs {
		if stored.Status != job.StatusRunning || stored.WorkerID != workerID {
			continue
		}
		q.failLocked(stored, stored.Result, "worker lost", now)
		recovered = append(recovered, stored.Clone())
	}
	return recovered
}

func (q *Queue) failLocked(stored *job.Job, result, failure string, now time.Time) {
	stored.Result = result
	stored.Error = failure
	if stored.Attempts <= stored.MaxRetries {
		stored.Status = job.StatusQueued
		stored.StartedAt = nil
		stored.CompletedAt = nil
		stored.WorkerID = ""
		nextAttempt := now.Add(q.retryDelay(stored.Attempts))
		stored.NextAttemptAt = &nextAttempt
		q.pushLocked(stored)
		return
	}

	stored.Status = job.StatusFailed
	stored.CompletedAt = &now
	stored.NextAttemptAt = nil
	stored.WorkerID = ""
}

func (q *Queue) retryDelay(attempt int) time.Duration {
	delay := q.retryBaseDelay
	for index := 1; index < attempt && delay < q.retryMaxDelay; index++ {
		delay *= 2
		if delay >= q.retryMaxDelay {
			return q.retryMaxDelay
		}
	}
	return delay
}

func (q *Queue) pushLocked(stored *job.Job) {
	heap.Push(&q.queued, &queueItem{
		jobID:     stored.ID,
		priority:  stored.Priority,
		createdAt: stored.CreatedAt,
	})
}

func (q *Queue) Cancel(id string) (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, exists := q.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}
	if stored.Status != job.StatusQueued && stored.Status != job.StatusRunning {
		return nil, ErrInvalidTransition
	}

	now := time.Now().UTC()
	stored.Status = job.StatusCancelled
	stored.CompletedAt = &now
	stored.NextAttemptAt = nil
	return stored.Clone(), nil
}

func (q *Queue) Get(id string) (*job.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, exists := q.jobs[id]
	if !exists {
		return nil, ErrJobNotFound
	}
	return stored.Clone(), nil
}

func (q *Queue) List() []*job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*job.Job, 0, len(q.jobs))
	for _, stored := range q.jobs {
		result = append(result, stored.Clone())
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

type queueItem struct {
	jobID     string
	priority  int
	createdAt time.Time
	index     int
}

type jobHeap []*queueItem

func (h jobHeap) Len() int {
	return len(h)
}

func (h jobHeap) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].createdAt.Before(h[j].createdAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *jobHeap) Push(value any) {
	item := value.(*queueItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *jobHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}
