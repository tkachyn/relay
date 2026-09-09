package api

import (
	"time"

	"github.com/tkachyn/relay/internal/job"
)

// create job request contains the fields accepted by the submission endpoint
type CreateJobRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	Timeout    string `json:"timeout"`
	RunAt      string `json:"run_at"`
}

// register worker request identifies the worker that is joining the server
type RegisterWorkerRequest struct {
	ID string `json:"id"`
}

// result request carries execution output or failure information back to the server
type ResultRequest struct {
	WorkerID string `json:"worker_id"`
	Success  bool   `json:"success"`
	Result   string `json:"result"`
	Error    string `json:"error"`
}

// worker records registration and liveness information for one worker
type Worker struct {
	ID           string    `json:"id"`
	RegisteredAt time.Time `json:"registered_at"`
	LastSeen     time.Time `json:"last_seen"`
	Status       string    `json:"status"`
}

// job list response wraps jobs to keep the API shape extensible
type JobListResponse struct {
	Jobs []*job.Job `json:"jobs"`
}

// worker list response wraps workers to keep the API shape extensible
type WorkerListResponse struct {
	Workers []Worker `json:"workers"`
}

// queue stats reports the current lifecycle counts for jobs
type QueueStats struct {
	Total     int `json:"total"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
}

// worker stats reports worker liveness totals
type WorkerStats struct {
	Total   int `json:"total"`
	Healthy int `json:"healthy"`
	Dead    int `json:"dead"`
}

// stats response combines operational counts with metric values
type StatsResponse struct {
	Queue   QueueStats         `json:"queue"`
	Workers WorkerStats        `json:"workers"`
	Metrics map[string]float64 `json:"metrics"`
}

// history response contains the ordered lifecycle events for one job
type HistoryResponse struct {
	Events []job.Event `json:"events"`
}

// error response provides a consistent message for failed API requests
type ErrorResponse struct {
	Error string `json:"error"`
}
