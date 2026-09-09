package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/job"
	"github.com/tkachyn/relay/internal/persistence"
	"github.com/tkachyn/relay/internal/queue"
)

type Server struct {
	queue            *queue.Queue
	workersMu        sync.Mutex
	workers          map[string]api.Worker
	jobSequence      atomic.Uint64
	store            *persistence.Store
	heartbeatTimeout time.Duration
	monitorInterval  time.Duration
}

type Options struct {
	StoragePath      string
	HeartbeatTimeout time.Duration
	MonitorInterval  time.Duration
	RetryBaseDelay   time.Duration
	RetryMaxDelay    time.Duration
}

func New() *Server {
	server, err := NewWithOptions(Options{})
	if err != nil {
		panic(err)
	}
	return server
}

func NewWithOptions(options Options) (*Server, error) {
	if options.HeartbeatTimeout <= 0 {
		options.HeartbeatTimeout = 10 * time.Second
	}
	if options.MonitorInterval <= 0 {
		options.MonitorInterval = time.Second
	}

	server := &Server{
		queue:            queue.NewWithOptions(queue.Options{RetryBaseDelay: options.RetryBaseDelay, RetryMaxDelay: options.RetryMaxDelay}),
		workers:          make(map[string]api.Worker),
		heartbeatTimeout: options.HeartbeatTimeout,
		monitorInterval:  options.MonitorInterval,
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

func (s *Server) Start(ctx context.Context) {
	go s.monitor(ctx)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/jobs", s.jobs)
	mux.HandleFunc("/v1/jobs/", s.jobByID)
	mux.HandleFunc("/v1/workers", s.workersList)
	mux.HandleFunc("/v1/workers/register", s.registerWorker)
	mux.HandleFunc("/v1/workers/", s.workerByID)
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
			if _, err := time.ParseDuration(request.Timeout); err != nil {
				writeError(w, http.StatusBadRequest, "timeout must be a valid duration")
				return
			}
		}

		now := time.Now().UTC()
		id := fmt.Sprintf("job-%d-%d", now.UnixNano(), s.jobSequence.Add(1))
		newJob := job.New(id, request.Type, request.Payload, request.Priority, now)
		newJob.MaxRetries = request.MaxRetries
		newJob.Timeout = request.Timeout
		if err := s.queue.Enqueue(newJob); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.persist(); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
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

func (s *Server) recoverFailures(now time.Time) {
	changed := len(s.queue.Expire(now)) > 0
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
		if len(s.queue.RecoverWorker(workerID)) > 0 {
			changed = true
		}
	}
	if len(lostWorkers) > 0 {
		changed = true
	}
	if changed {
		_ = s.persist()
	}
}

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
	return s.store.Save(persistence.State{
		Jobs:    s.queue.List(),
		Workers: workers,
	})
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
	if err := json.NewDecoder(r.Body).Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
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
