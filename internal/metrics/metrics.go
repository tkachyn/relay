package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// histogram is the JSON-safe view of an observed latency distribution
type Histogram struct {
	Count   uint64            `json:"count"`
	Sum     float64           `json:"sum"`
	Buckets map[string]uint64 `json:"buckets"`
}

// snapshot is a consistent copy of all metric values
type Snapshot struct {
	Counters   map[string]uint64    `json:"counters"`
	Gauges     map[string]float64   `json:"gauges"`
	Histograms map[string]Histogram `json:"histograms"`
}

type histogram struct {
	bounds []float64
	counts []uint64
	count  uint64
	sum    float64
}

// metrics collects counters, gauges, and bounded latency histograms safely
type Metrics struct {
	mu         sync.Mutex
	counters   map[string]uint64
	gauges     map[string]float64
	histograms map[string]*histogram
}

// new creates an empty metrics registry
func New() *Metrics {
	return &Metrics{
		counters:   make(map[string]uint64),
		gauges:     make(map[string]float64),
		histograms: make(map[string]*histogram),
	}
}

// inc increases a named counter by one
func (m *Metrics) Inc(name string) {
	m.Add(name, 1)
}

// add increases a named counter by an arbitrary amount
func (m *Metrics) Add(name string, amount uint64) {
	m.mu.Lock()
	m.counters[name] += amount
	m.mu.Unlock()
}

// set records the current value of a named gauge
func (m *Metrics) Set(name string, value float64) {
	m.mu.Lock()
	m.gauges[name] = value
	m.mu.Unlock()
}

// observe records a value in a bounded latency histogram
func (m *Metrics) Observe(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, exists := m.histograms[name]
	if !exists {
		current = &histogram{
			bounds: []float64{0.001, 0.01, 0.1, 1, 10, 60},
			counts: make([]uint64, 7),
		}
		m.histograms[name] = current
	}
	current.count++
	current.sum += value
	for i, bound := range current.bounds {
		if value <= bound {
			current.counts[i]++
		}
	}
	current.counts[len(current.bounds)]++
}

// snapshot returns a copy that can be serialized without holding the lock
func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := Snapshot{
		Counters:   make(map[string]uint64, len(m.counters)),
		Gauges:     make(map[string]float64, len(m.gauges)),
		Histograms: make(map[string]Histogram, len(m.histograms)),
	}
	for name, value := range m.counters {
		snapshot.Counters[name] = value
	}
	for name, value := range m.gauges {
		snapshot.Gauges[name] = value
	}
	for name, current := range m.histograms {
		buckets := make(map[string]uint64, len(current.counts))
		for index, count := range current.counts {
			if index == len(current.bounds) {
				buckets["+Inf"] = count
			} else {
				buckets[fmt.Sprintf("%g", current.bounds[index])] = count
			}
		}
		snapshot.Histograms[name] = Histogram{
			Count:   current.count,
			Sum:     current.sum,
			Buckets: buckets,
		}
	}
	return snapshot
}

// prometheus renders the registry in text exposition format
func (m *Metrics) Prometheus() string {
	snapshot := m.Snapshot()
	var builder strings.Builder

	counterNames := sortedKeys(snapshot.Counters)
	for _, name := range counterNames {
		fmt.Fprintf(&builder, "# TYPE %s counter\n%s %d\n", name, name, snapshot.Counters[name])
	}
	gaugeNames := sortedFloatKeys(snapshot.Gauges)
	for _, name := range gaugeNames {
		fmt.Fprintf(&builder, "# TYPE %s gauge\n%s %g\n", name, name, snapshot.Gauges[name])
	}
	histogramNames := sortedHistogramKeys(snapshot.Histograms)
	for _, name := range histogramNames {
		histogram := snapshot.Histograms[name]
		fmt.Fprintf(&builder, "# TYPE %s histogram\n", name)
		bounds := make([]string, 0, len(histogram.Buckets))
		for bound := range histogram.Buckets {
			bounds = append(bounds, bound)
		}
		sort.Slice(bounds, func(i, j int) bool {
			if bounds[i] == "+Inf" {
				return false
			}
			if bounds[j] == "+Inf" {
				return true
			}
			left, _ := strconv.ParseFloat(bounds[i], 64)
			right, _ := strconv.ParseFloat(bounds[j], 64)
			return left < right
		})
		for _, bound := range bounds {
			count := histogram.Buckets[bound]
			fmt.Fprintf(&builder, "%s_bucket{le=\"%s\"} %d\n", name, bound, count)
		}
		fmt.Fprintf(&builder, "%s_sum %g\n%s_count %d\n", name, histogram.Sum, name, histogram.Count)
	}
	return builder.String()
}

func sortedKeys(values map[string]uint64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedFloatKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedHistogramKeys(values map[string]Histogram) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
