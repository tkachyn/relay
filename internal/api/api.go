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

// error response provides a consistent message for failed API requests
type ErrorResponse struct {
	Error string `json:"error"`
}
