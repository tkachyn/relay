package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/client"
	"github.com/tkachyn/relay/internal/queue"
	"github.com/tkachyn/relay/internal/server"
	"github.com/tkachyn/relay/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "server":
		err = runServer(os.Args[2:])
	case "worker":
		err = runWorker(os.Args[2:])
	case "submit":
		err = runSubmit(os.Args[2:])
	case "jobs":
		err = runJobs(os.Args[2:])
	case "job":
		err = runJob(os.Args[2:])
	case "history":
		err = runHistory(os.Args[2:])
	case "cancel":
		err = runCancel(os.Args[2:])
	case "workers":
		err = runWorkers(os.Args[2:])
	case "stats":
		err = runStats(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServer(args []string) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	listen := flags.String("listen", "127.0.0.1:8080", "address to listen on")
	dataPath := flags.String("data", "relay-state.json", "path to the persistent state file; empty disables persistence")
	heartbeatTimeout := flags.Duration("heartbeat-timeout", 10*time.Second, "time before an unresponsive worker is considered dead")
	monitorInterval := flags.Duration("monitor-interval", time.Second, "interval for timeout and worker failure checks")
	retryBaseDelay := flags.Duration("retry-base-delay", time.Second, "initial retry delay")
	retryMaxDelay := flags.Duration("retry-max-delay", time.Minute, "maximum retry delay")
	schedulingPolicy := flags.String("scheduling-policy", string(queue.PolicyPriority), "job scheduling policy: priority or fifo")
	pprofListen := flags.String("pprof", "", "optional address for Go pprof endpoints")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *schedulingPolicy != string(queue.PolicyPriority) && *schedulingPolicy != string(queue.PolicyFIFO) {
		return errors.New("scheduling-policy must be priority or fifo")
	}

	relayServer, err := server.NewWithOptions(server.Options{
		StoragePath:      *dataPath,
		HeartbeatTimeout: *heartbeatTimeout,
		MonitorInterval:  *monitorInterval,
		RetryBaseDelay:   *retryBaseDelay,
		RetryMaxDelay:    *retryMaxDelay,
		SchedulingPolicy: queue.Policy(*schedulingPolicy),
		Logger:           slog.New(slog.NewTextHandler(os.Stdout, nil)),
	})
	if err != nil {
		return fmt.Errorf("load server state: %w", err)
	}
	httpServer := &http.Server{
		Addr:    *listen,
		Handler: relayServer.Handler(),
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	relayServer.Start(ctx)
	if *pprofListen != "" {
		profileServer := &http.Server{Addr: *pprofListen, Handler: http.DefaultServeMux}
		go func() {
			<-ctx.Done()
			_ = profileServer.Shutdown(context.Background())
		}()
		go func() {
			if profileErr := profileServer.ListenAndServe(); profileErr != nil && !errors.Is(profileErr, http.ErrServerClosed) {
				fmt.Fprintln(os.Stderr, "pprof server:", profileErr)
			}
		}()
	}
	go func() {
		<-ctx.Done()
		_ = httpServer.Shutdown(context.Background())
	}()

	err = httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runWorker(args []string) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	workerID := flags.String("id", worker.DefaultID(), "worker id")
	pollPeriod := flags.Duration("poll", 500*time.Millisecond, "poll interval when no job is available")
	if err := flags.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return worker.New(*workerID, *serverURL, *pollPeriod).Run(ctx)
}

func runSubmit(args []string) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	jobType := flags.String("type", "command", "job type")
	priority := flags.Int("priority", 0, "job priority")
	maxRetries := flags.Int("max-retries", 0, "maximum number of retries after a failure")
	timeout := flags.String("timeout", "", "maximum execution duration, such as 30s")
	runAt := flags.String("run-at", "", "RFC3339 timestamp when execution may begin")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) == 0 {
		return errors.New("submit requires a command")
	}

	current, err := client.New(*serverURL).Submit(api.CreateJobRequest{
		Type:       *jobType,
		Payload:    strings.Join(flags.Args(), " "),
		Priority:   *priority,
		MaxRetries: *maxRetries,
		Timeout:    *timeout,
		RunAt:      *runAt,
	})
	if err != nil {
		return err
	}
	fmt.Println(current.ID)
	return nil
}

func runJobs(args []string) error {
	flags := flag.NewFlagSet("jobs", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	jobs, err := client.New(*serverURL).ListJobs()
	if err != nil {
		return err
	}
	fmt.Println("ID\tSTATUS\tPRIORITY\tATTEMPTS\tWORKER")
	for _, current := range jobs {
		fmt.Printf("%s\t%s\t%d\t%d\t%s\n", current.ID, current.Status, current.Priority, current.Attempts, current.WorkerID)
	}
	return nil
}

func runJob(args []string) error {
	flags := flag.NewFlagSet("job", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("job requires an id")
	}
	current, err := client.New(*serverURL).GetJob(flags.Args()[0])
	if err != nil {
		return err
	}
	return printJSON(current)
}

func runCancel(args []string) error {
	flags := flag.NewFlagSet("cancel", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("cancel requires an id")
	}
	current, err := client.New(*serverURL).CancelJob(flags.Args()[0])
	if err != nil {
		return err
	}
	fmt.Println(current.ID)
	return nil
}

func runHistory(args []string) error {
	flags := flag.NewFlagSet("history", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 1 {
		return errors.New("history requires an id")
	}
	events, err := client.New(*serverURL).GetHistory(flags.Args()[0])
	if err != nil {
		return err
	}
	return printJSON(events)
}

func runWorkers(args []string) error {
	flags := flag.NewFlagSet("workers", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	workers, err := client.New(*serverURL).ListWorkers()
	if err != nil {
		return err
	}
	fmt.Println("ID\tREGISTERED\tLAST SEEN")
	for _, current := range workers {
		fmt.Printf("%s\t%s\t%s\n", current.ID, current.RegisteredAt.Format(time.RFC3339), current.LastSeen.Format(time.RFC3339))
	}
	return nil
}

func runStats(args []string) error {
	flags := flag.NewFlagSet("stats", flag.ContinueOnError)
	serverURL := flags.String("server", "http://127.0.0.1:8080", "relay server URL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	stats, err := client.New(*serverURL).GetStats()
	if err != nil {
		return err
	}
	return printJSON(stats)
}

func printJSON(value any) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: relay <server|worker|submit|jobs|job|history|cancel|workers|stats>")
}
