package metrics

import (
	"strings"
	"testing"
)

func TestMetricsSnapshotAndPrometheus(t *testing.T) {
	metrics := New()
	metrics.Inc("relay_jobs_completed_total")
	metrics.Set("relay_queue_depth", 3)
	metrics.Observe("relay_job_execution_seconds", 0.02)
	metrics.Observe("relay_job_execution_seconds", 2)

	snapshot := metrics.Snapshot()
	if snapshot.Counters["relay_jobs_completed_total"] != 1 {
		t.Fatalf("unexpected counters: %+v", snapshot.Counters)
	}
	if snapshot.Gauges["relay_queue_depth"] != 3 {
		t.Fatalf("unexpected gauges: %+v", snapshot.Gauges)
	}
	if snapshot.Histograms["relay_job_execution_seconds"].Count != 2 {
		t.Fatalf("unexpected histogram: %+v", snapshot.Histograms)
	}

	output := metrics.Prometheus()
	for _, expected := range []string{
		"relay_jobs_completed_total 1",
		"relay_queue_depth 3",
		"relay_job_execution_seconds_count 2",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("missing %q in metrics output:\n%s", expected, output)
		}
	}
}
