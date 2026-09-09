package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/client"
)

type Worker struct {
	ID         string
	Client     *client.Client
	PollPeriod time.Duration
}

func New(id, serverURL string, pollPeriod time.Duration) *Worker {
	return &Worker{
		ID:         id,
		Client:     client.New(serverURL),
		PollPeriod: pollPeriod,
	}
}

func DefaultID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func (w *Worker) Run(ctx context.Context) error {
	if _, err := w.Client.RegisterWorker(w.ID); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

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

		result, runErr := execute(ctx, current.Payload)
		request := api.ResultRequest{
			WorkerID: w.ID,
			Success:  runErr == nil,
			Result:   result,
		}
		if runErr != nil {
			request.Error = runErr.Error()
		}
		if _, err := w.Client.ReportResult(current.ID, request); err != nil {
			return fmt.Errorf("report result for %s: %w", current.ID, err)
		}
	}
}

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
