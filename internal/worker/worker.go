package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/client"
)

// worker polls for work while a separate loop keeps its registration alive
type Worker struct {
	ID              string
	Client          *client.Client
	PollPeriod      time.Duration
	HeartbeatPeriod time.Duration
}

// new creates a worker client with a one-second heartbeat by default
func New(id, serverURL string, pollPeriod time.Duration) *Worker {
	return &Worker{
		ID:              id,
		Client:          client.New(serverURL),
		PollPeriod:      pollPeriod,
		HeartbeatPeriod: time.Second,
	}
}

// default id builds a stable local worker identifier
func DefaultID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// run claims, executes, and reports jobs until the context is cancelled
func (w *Worker) Run(ctx context.Context) error {
	if _, err := w.Client.RegisterWorker(w.ID); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go w.heartbeatLoop(heartbeatCtx, heartbeatDone)
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		current, found, err := w.Client.ClaimJob(w.ID)
		if err != nil {
			return fmt.Errorf("claim job: %w", err)
		}
		if !found {
			if err := wait(ctx, w.PollPeriod); err != nil {
				return nil
			}
			continue
		}

		jobContext, cancel := context.WithCancel(ctx)
		if current.Timeout != "" {
			timeout, parseErr := time.ParseDuration(current.Timeout)
			if parseErr != nil {
				cancel()
				return fmt.Errorf("parse timeout for %s: %w", current.ID, parseErr)
			}
			jobContext, cancel = context.WithTimeout(ctx, timeout)
		}
		result, runErr := execute(jobContext, current.Payload)
		cancel()
		request := api.ResultRequest{
			WorkerID: w.ID,
			Success:  runErr == nil,
			Result:   result,
		}
		if runErr != nil {
			request.Error = runErr.Error()
		}
		if _, err := w.Client.ReportResult(current.ID, request); err != nil {
			var responseErr *client.HTTPError
			if errors.As(err, &responseErr) &&
				(responseErr.StatusCode == 404 || responseErr.StatusCode == 409) {
				continue
			}
			return fmt.Errorf("report result for %s: %w", current.ID, err)
		}
	}
}

// heartbeat loop lets the server distinguish a live worker from a lost one
func (w *Worker) heartbeatLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	heartbeatPeriod := w.HeartbeatPeriod
	if heartbeatPeriod <= 0 {
		heartbeatPeriod = time.Second
	}
	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.Client.Heartbeat(w.ID)
		}
	}
}

// execute runs the submitted command through the host operating system shell
func execute(ctx context.Context, command string) (string, error) {
	var commandLine *exec.Cmd
	if runtime.GOOS == "windows" {
		commandLine = exec.CommandContext(ctx, "cmd", "/C", command)
	} else {
		commandLine = exec.CommandContext(ctx, "sh", "-c", command)
	}

	output, err := commandLine.CombinedOutput()
	result := strings.TrimSpace(string(output))
	if err != nil {
		return result, err
	}
	return result, nil
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
