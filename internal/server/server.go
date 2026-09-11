package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/dashboard"
	"github.com/tkachyn/relay/internal/job"
	"github.com/tkachyn/relay/internal/metrics"
	"github.com/tkachyn/relay/internal/persistence"
	"github.com/tkachyn/relay/internal/queue"
)

// server coordinates queue transitions, worker liveness, and persistence
type Server struct {
	queue            *queue.Queue
	workersMu        sync.Mutex
	workers          map[string]api.Worker
	jobSequence      atomic.Uint64
	store            *persistence.Store
	heartbeatTimeout time.Duration
	monitorInterval  time.Duration
	metrics          *metrics.Metrics
	logger           *slog.Logger
}

// options controls recovery timing and the on-disk state location
type Options struct {
	StoragePath      string
	HeartbeatTimeout time.Duration
	MonitorInterval  time.Duration
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
	SchedulingPolicy queue.Policy
	Metrics          *metrics.Metrics
	Logger           *slog.Logger
}

const maxJSONBodySize = 1 << 20

// new creates an in-memory server with default recovery settings
func New() *Server {
	server, err := NewWithOptions(Options{})
	if err != nil {
		panic(err)
	}
	return server
}

// new with options loads persisted state before serving requests
func NewWithOptions(options Options) (*Server, error) {
	if options.HeartbeatTimeout <= 0 {
		options.HeartbeatTimeout = 10 * time.Second
	}
	if options.MonitorInterval <= 0 {
		options.MonitorInterval = time.Second
	}

	server := &Server{
		queue: queue.NewWithOptions(queue.Options{
			RetryBaseDelay: options.RetryBaseDelay,
			RetryMaxDelay:  options.RetryMaxDelay,
			Policy:         options.SchedulingPolicy,
		}),
		workers:          make(map[string]api.Worker),
		heartbeatTimeout: options.HeartbeatTimeout,
		monitorInterval:  options.MonitorInterval,
		metrics:          options.Metrics,
		logger:           options.Logger,
	}
	if server.metrics == nil {
		server.metrics = metrics.New()
	}
	if server.logger == nil {
		server.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if options.StoragePath == "" {
		return server, nil
	}

	server.store = persistence.New(options.StoragePath)
	state, err := server.store.Load()
	if err != nil {
		return nil, err
	}
	if err := server.queue.Restore(state.Jobs); err != nil {
		return nil, err
	}
	for _, worker := range state.Workers {
		worker.Status = "dead"
		server.workers[worker.ID] = worker
	}
	return server, nil
}

// start begins timeout and heartbeat monitoring until the context ends
func (s *Server) Start(ctx context.Context) {
	go s.monitor(ctx)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/metrics", s.metricsHandler)
	mux.HandleFunc("/v1/stats", s.stats)
	mux.HandleFunc("/v1/jobs", s.jobs)
	mux.HandleFunc("/v1/jobs/", s.jobByID)
	mux.HandleFunc("/v1/workers", s.workersList)
	mux.HandleFunc("/v1/workers/register", s.registerWorker)
	mux.HandleFunc("/v1/workers/", s.workerByID)
	mux.Handle("/dashboard/", dashboard.Handler())
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.refreshMetrics()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(s.metrics.Prometheus()))
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.refreshMetrics()
	writeJSON(w, http.StatusOK, s.statistics())
}

func (s *Server) jobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, api.JobListResponse{Jobs: s.queue.List()})
	case http.MethodPost:
		var request api.CreateJobRequest
		if !decodeJSON(w, r, &request) {
			return
		}
		if request.Type == "" {
			request.Type = "command"
		}
		if request.Payload == "" {
			writeError(w, http.StatusBadRequest, "payload is required")
			return
		}
		if request.MaxRetries < 0 {
			writeError(w, http.StatusBadRequest, "max_retries must not be negative")
			return
		}
		if request.Timeout != "" {
			timeout, err := time.ParseDuration(request.Timeout)
			if err != nil || timeout <= 0 {
				writeError(w, http.StatusBadRequest, "timeout must be a positive duration")
				return
			}
		}
		var scheduledAt *time.Time
		if request.RunAt != "" {
			parsed, err := time.Parse(time.RFC3339, request.RunAt)
			if err != nil {
				writeError(w, http.StatusBadRequest, "run_at must be an RFC3339 timestamp")
				return
			}
			parsed = parsed.UTC()
			scheduledAt = &parsed
		}

		now := time.Now().UTC()
		id := fmt.Sprintf("job-%d-%d", now.UnixNano(), s.jobSequence.Add(1))
		newJob := job.New(id, request.Type, request.Payload, request.Priority, now)
		newJob.MaxRetries = request.MaxRetries
		newJob.Timeout = request.Timeout
		newJob.ScheduledAt = scheduledAt
		if err := s.queue.Enqueue(newJob); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.persist(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.metrics.Inc("relay_jobs_submitted_total")
		s.logger.Info("job submitted", "job_id", newJob.ID, "priority", newJob.Priority, "scheduled_at", newJob.ScheduledAt)
		writeJSON(w, http.StatusCreated, newJob)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) jobByID(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(strings.TrimPrefix(r.URL.Path, "/v1/jobs/"))
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}

	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		current, err := s.queue.Get(id)
		if err != nil {
			writeQueueError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, current)
		return
	}

	if len(parts) == 2 && parts[1] == "history" && r.Method == http.MethodGet {
		current, err := s.queue.Get(id)
		if err != nil {
			writeQueueError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, api.HistoryResponse{Events: current.History})
		return
	}

	if len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		current, err := s.queue.Cancel(id)
		if err != nil {
			writeQueueError(w, err)
			return
		}
		if err := s.persist(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.metrics.Inc("relay_jobs_cancelled_total")
		s.logger.Info("job cancelled", "job_id", id)
		writeJSON(w, http.StatusOK, current)
		return
	}

	if len(parts) == 2 && parts[1] == "result" && r.Method == http.MethodPost {
		s.result(w, r, id)
		return
	}

	writeError(w, http.StatusNotFound, "route not found")
}

func (s *Server) result(w http.ResponseWriter, r *http.Request, id string) {
	var request api.ResultRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "worker_id is required")
		return
	}
	if !s.touchWorker(request.WorkerID) {
		writeError(w, http.StatusNotFound, "worker not registered")
		return
	}

	var (
		current *job.Job
		err     error
	)
	if request.Success {
		current, err = s.queue.Complete(id, request.WorkerID, request.Result)
	} else {
		current, err = s.queue.Fail(id, request.WorkerID, request.Result, request.Error)
	}
	if err != nil {
		writeQueueError(w, err)
		return
	}
	if err := s.persist(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if current.Status == job.StatusQueued {
		s.metrics.Inc("relay_jobs_retried_total")
		s.logger.Warn("job retry scheduled", "job_id", id, "attempts", current.Attempts, "error", current.Error)
	} else if current.Status == job.StatusCompleted {
		s.metrics.Inc("relay_jobs_completed_total")
		s.logger.Info("job completed", "job_id", id, "worker_id", request.WorkerID)
	} else {
		s.metrics.Inc("relay_jobs_failed_total")
		s.logger.Warn("job failed", "job_id", id, "worker_id", request.WorkerID, "error", current.Error)
	}
	writeJSON(w, http.StatusOK, current)
}

func (s *Server) registerWorker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var request api.RegisterWorkerRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ID == "" {
		writeError(w, http.StatusBadRequest, "worker id is required")
		return
	}

	now := time.Now().UTC()
	s.workersMu.Lock()
	registered, exists := s.workers[request.ID]
	if !exists {
		registered = api.Worker{ID: request.ID, RegisteredAt: now}
	}
	registered.LastSeen = now
	registered.Status = "healthy"
	s.workers[request.ID] = registered
	s.workersMu.Unlock()
	if err := s.persist(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !exists {
		s.metrics.Inc("relay_workers_registered_total")
	}
	writeJSON(w, http.StatusOK, registered)
}

func (s *Server) workersList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.workersMu.Lock()
	workers := make([]api.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	s.workersMu.Unlock()
	writeJSON(w, http.StatusOK, api.WorkerListResponse{Workers: workers})
}

func (s *Server) workerByID(w http.ResponseWriter, r *http.Request) {
	parts := pathParts(strings.TrimPrefix(r.URL.Path, "/v1/workers/"))
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}

	workerID := parts[0]
	if parts[1] == "heartbeat" {
		s.heartbeat(w, workerID)
		return
	}
	if parts[1] != "claim" {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}
	if !s.workerExists(workerID) {
		writeError(w, http.StatusNotFound, "worker not registered")
		return
	}
	s.touchWorker(workerID)

	current, ok := s.queue.Claim(workerID)
	if !ok {
		if err := s.persist(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := s.persist(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.metrics.Inc("relay_jobs_claimed_total")
	s.metrics.Observe("relay_job_queue_latency_seconds", time.Since(current.CreatedAt).Seconds())
	s.logger.Info("job claimed", "job_id", current.ID, "worker_id", workerID)
	writeJSON(w, http.StatusOK, current)
}

func (s *Server) heartbeat(w http.ResponseWriter, workerID string) {
	if !s.recordHeartbeat(workerID) {
		writeError(w, http.StatusNotFound, "worker not registered")
		return
	}
	if err := s.persist(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) workerExists(id string) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	worker, exists := s.workers[id]
	return exists && worker.Status == "healthy"
}

func (s *Server) touchWorker(id string) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	current, exists := s.workers[id]
	if exists && current.Status == "healthy" {
		current.LastSeen = time.Now().UTC()
		s.workers[id] = current
	}
	return exists && current.Status == "healthy"
}

func (s *Server) recordHeartbeat(id string) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	current, exists := s.workers[id]
	if !exists {
		return false
	}
	current.LastSeen = time.Now().UTC()
	current.Status = "healthy"
	s.workers[id] = current
	return true
}

func (s *Server) monitor(ctx context.Context) {
	ticker := time.NewTicker(s.monitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.recoverFailures(now.UTC())
		}
	}
}

// recover failures converts expired attempts and dead workers into retries
func (s *Server) recoverFailures(now time.Time) {
	expired := s.queue.Expire(now)
	changed := len(expired) > 0
	for _, current := range expired {
		s.metrics.Inc("relay_jobs_timed_out_total")
		s.logger.Warn("job timed out", "job_id", current.ID, "attempts", current.Attempts)
	}
	var lostWorkers []string

	s.workersMu.Lock()
	for id, worker := range s.workers {
		if worker.Status != "healthy" || worker.LastSeen.IsZero() || now.Sub(worker.LastSeen) <= s.heartbeatTimeout {
			continue
		}
		worker.Status = "dead"
		s.workers[id] = worker
		lostWorkers = append(lostWorkers, id)
	}
	s.workersMu.Unlock()

	for _, workerID := range lostWorkers {
		recovered := s.queue.RecoverWorker(workerID)
		if len(recovered) > 0 {
			changed = true
			s.metrics.Inc("relay_worker_failures_total")
			s.metrics.Add("relay_jobs_recovered_total", uint64(len(recovered)))
			s.logger.Warn("worker lost", "worker_id", workerID, "jobs_recovered", len(recovered))
		}
	}
	if len(lostWorkers) > 0 {
		changed = true
	}
	if changed {
		_ = s.persist()
	}
}

func (s *Server) refreshMetrics() {
	queueStats := s.queue.Statistics()
	s.metrics.Set("relay_queue_depth", float64(queueStats.Queued))
	s.metrics.Set("relay_running_jobs", float64(queueStats.Running))

	s.workersMu.Lock()
	healthy := 0
	dead := 0
	for _, worker := range s.workers {
		if worker.Status == "healthy" {
			healthy++
		} else {
			dead++
		}
	}
	s.workersMu.Unlock()
	s.metrics.Set("relay_active_workers", float64(healthy))
	s.metrics.Set("relay_dead_workers", float64(dead))
}

func (s *Server) statistics() api.StatsResponse {
	queueStats := s.queue.Statistics()
	s.workersMu.Lock()
	workerStats := api.WorkerStats{Total: len(s.workers)}
	for _, worker := range s.workers {
		if worker.Status == "healthy" {
			workerStats.Healthy++
		} else {
			workerStats.Dead++
		}
	}
	s.workersMu.Unlock()

	snapshot := s.metrics.Snapshot()
	values := make(map[string]float64, len(snapshot.Counters)+len(snapshot.Gauges))
	for name, value := range snapshot.Counters {
		values[name] = float64(value)
	}
	for name, value := range snapshot.Gauges {
		values[name] = value
	}
	return api.StatsResponse{
		Queue: api.QueueStats{
			Total:     queueStats.Total,
			Queued:    queueStats.Queued,
			Running:   queueStats.Running,
			Completed: queueStats.Completed,
			Failed:    queueStats.Failed,
			Cancelled: queueStats.Cancelled,
		},
		Workers: workerStats,
		Metrics: values,
	}
}

// persist saves jobs and workers as one state snapshot
func (s *Server) persist() error {
	if s.store == nil {
		return nil
	}

	s.workersMu.Lock()
	workers := make([]api.Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	s.workersMu.Unlock()
	err := s.store.Save(persistence.State{
		Jobs:    s.queue.List(),
		Workers: workers,
	})
	if err != nil {
		s.logger.Error("persist state failed", "error", err)
	}
	return err
}

func writeQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrJobNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrInvalidTransition):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxJSONBodySize))
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid json")
		return false
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain one JSON value")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, api.ErrorResponse{Error: message})
}

func pathParts(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}
