package api

import (
	"time"

	"github.com/tkachyn/relay/internal/job"
)

type CreateJobRequest struct {
	Type       string `json:"type"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	Timeout    string `json:"timeout"`
}

type RegisterWorkerRequest struct {
	ID string `json:"id"`
}

type ResultRequest struct {
	WorkerID string `json:"worker_id"`
	Success  bool   `json:"success"`
	Result   string `json:"result"`
	Error    string `json:"error"`
}

type Worker struct {
	ID           string    `json:"id"`
	RegisteredAt time.Time `json:"registered_at"`
	LastSeen     time.Time `json:"last_seen"`
	Status       string    `json:"status"`
}

type JobListResponse struct {
	Jobs []*job.Job `json:"jobs"`
}

type WorkerListResponse struct {
	Workers []Worker `json:"workers"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}
