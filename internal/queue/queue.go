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
	mu     sync.Mutex
	jobs   map[string]*job.Job
	queued jobHeap
}

func New() *Queue {
	return &Queue{
		jobs: make(map[string]*job.Job),
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
	heap.Push(&q.queued, &queueItem{jobID: stored.ID, priority: stored.Priority, createdAt: stored.CreatedAt})
	return nil
}

func (q *Queue) Claim(workerID string) (*job.Job, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.queued.Len() > 0 {
		item := heap.Pop(&q.queued).(*queueItem)
		stored, exists := q.jobs[item.jobID]
		if !exists || stored.Status != job.StatusQueued {
			continue
		}

		now := time.Now().UTC()
		stored.Status = job.StatusRunning
		stored.StartedAt = &now
		stored.Attempts++
		stored.WorkerID = workerID
		return stored.Clone(), true
	}

	return nil, false
}

func (q *Queue) Complete(id, workerID, result string) (*job.Job, error) {
	return q.finish(id, workerID, job.StatusCompleted, result, "")
}

func (q *Queue) Fail(id, workerID, result, failure string) (*job.Job, error) {
	return q.finish(id, workerID, job.StatusFailed, result, failure)
}

func (q *Queue) finish(id, workerID string, status job.Status, result, failure string) (*job.Job, error) {
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
	stored.Status = status
	stored.CompletedAt = &now
	stored.Result = result
	stored.Error = failure
	return stored.Clone(), nil
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
