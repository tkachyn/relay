package job

import "time"

// status identifies a job's position in its lifecycle
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// job contains the durable metadata needed to schedule and recover work
type Job struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	Payload       string     `json:"payload"`
	Priority      int        `json:"priority"`
	Status        Status     `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	Attempts      int        `json:"attempts"`
	MaxRetries    int        `json:"max_retries"`
	Timeout       string     `json:"timeout,omitempty"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	WorkerID      string     `json:"worker_id,omitempty"`
	Result        string     `json:"result,omitempty"`
	Error         string     `json:"error,omitempty"`
}

// new creates a queued job with its initial scheduling metadata
func New(id, jobType, payload string, priority int, createdAt time.Time) *Job {
	return &Job{
		ID:        id,
		Type:      jobType,
		Payload:   payload,
		Priority:  priority,
		Status:    StatusQueued,
		CreatedAt: createdAt,
	}
}

// clone prevents callers from modifying queue-owned timestamps and state
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}

	clone := *j
	if j.StartedAt != nil {
		startedAt := *j.StartedAt
		clone.StartedAt = &startedAt
	}
	if j.CompletedAt != nil {
		completedAt := *j.CompletedAt
		clone.CompletedAt = &completedAt
	}
	if j.NextAttemptAt != nil {
		nextAttemptAt := *j.NextAttemptAt
		clone.NextAttemptAt = &nextAttemptAt
	}
	return &clone
}
