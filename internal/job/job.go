package job

import "time"

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Job struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`
	Payload     string     `json:"payload"`
	Priority    int        `json:"priority"`
	Status      Status     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Attempts    int        `json:"attempts"`
	WorkerID    string     `json:"worker_id,omitempty"`
	Result      string     `json:"result,omitempty"`
	Error       string     `json:"error,omitempty"`
}

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
	return &clone
}
