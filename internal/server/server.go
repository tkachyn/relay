package server

import (
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
	"github.com/tkachyn/relay/internal/queue"
)

type Server struct {
	queue       *queue.Queue
	workersMu   sync.Mutex
	workers     map[string]api.Worker
	jobSequence atomic.Uint64
}

func New() *Server {
	return &Server{
		queue:   queue.New(),
		workers: make(map[string]api.Worker),
	}
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

		now := time.Now().UTC()
		id := fmt.Sprintf("job-%d-%d", now.UnixNano(), s.jobSequence.Add(1))
		newJob := job.New(id, request.Type, request.Payload, request.Priority, now)
		if err := s.queue.Enqueue(newJob); err != nil {
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
	s.touchWorker(request.WorkerID)

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
	s.workers[request.ID] = registered
	s.workersMu.Unlock()
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
	if len(parts) != 2 || parts[1] != "claim" || r.Method != http.MethodPost {
		writeError(w, http.StatusNotFound, "route not found")
		return
	}

	workerID := parts[0]
	if !s.workerExists(workerID) {
		writeError(w, http.StatusNotFound, "worker not registered")
		return
	}
	s.touchWorker(workerID)

	current, ok := s.queue.Claim(workerID)
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, current)
}

func (s *Server) workerExists(id string) bool {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	_, exists := s.workers[id]
	return exists
}

func (s *Server) touchWorker(id string) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	current, exists := s.workers[id]
	if exists {
		current.LastSeen = time.Now().UTC()
		s.workers[id] = current
	}
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
