package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Labels map[string]string

type Recorder interface {
	IncCounter(name string, labels Labels)
	AddCounter(name string, labels Labels, value float64)
	SetGauge(name string, labels Labels, value float64)
	ObserveHistogram(name string, labels Labels, value float64)
}

type Registry struct {
	mu         sync.Mutex
	counters   map[string]float64
	gauges     map[string]float64
	histograms map[string]*histogramSample
	buckets    []float64
}

type histogramSample struct {
	buckets []uint64
	count   uint64
	sum     float64
}

type metricDefinition struct {
	kind string
	help string
}

var definitions = map[string]metricDefinition{
	"processing_kafka_messages_total": {
		kind: "counter",
		help: "Kafka messages handled by processing, labeled by status.",
	},
	"processing_kafka_commits_total": {
		kind: "counter",
		help: "Kafka offset commit attempts, labeled by status.",
	},
	"processing_dlq_messages_total": {
		kind: "counter",
		help: "Kafka messages written to the processing DLQ, labeled by reason.",
	},
	"processing_alerts_created_total": {
		kind: "counter",
		help: "Alerts created by processing, labeled by alert type.",
	},
	"processing_errors_total": {
		kind: "counter",
		help: "Processing errors, labeled by pipeline stage.",
	},
	"processing_kafka_consumer_lag": {
		kind: "gauge",
		help: "Approximate Kafka consumer lag from the last fetched message high watermark.",
	},
	"processing_processing_duration_seconds": {
		kind: "histogram",
		help: "End-to-end processing duration for one Kafka message.",
	},
	"processing_postgres_write_duration_seconds": {
		kind: "histogram",
		help: "PostgreSQL write latency, labeled by operation.",
	},
	"processing_redis_duration_seconds": {
		kind: "histogram",
		help: "Redis operation latency, labeled by operation.",
	},
	"processing_sliding_window_events_current": {
		kind: "gauge",
		help: "Number of events in the latest processed sliding window.",
	},
}

func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]float64),
		gauges:     make(map[string]float64),
		histograms: make(map[string]*histogramSample),
		buckets: []float64{
			0.001,
			0.005,
			0.01,
			0.025,
			0.05,
			0.1,
			0.25,
			0.5,
			1,
			2.5,
			5,
			10,
		},
	}
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = r.WriteTo(w)
	})
}

func (r *Registry) IncCounter(name string, labels Labels) {
	r.AddCounter(name, labels, 1)
}

func (r *Registry) AddCounter(name string, labels Labels, value float64) {
	if value < 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.counters[metricKey(name, labels)] += value
}

func (r *Registry) SetGauge(name string, labels Labels, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.gauges[metricKey(name, labels)] = value
}

func (r *Registry) ObserveHistogram(name string, labels Labels, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := metricKey(name, labels)
	sample := r.histograms[key]
	if sample == nil {
		sample = &histogramSample{
			buckets: make([]uint64, len(r.buckets)),
		}
		r.histograms[key] = sample
	}

	for i, bucket := range r.buckets {
		if value <= bucket {
			sample.buckets[i]++
		}
	}
	sample.count++
	sample.sum += value
}

func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	snapshot := r.snapshotLocked()
	r.mu.Unlock()

	var written int64
	write := func(format string, args ...interface{}) error {
		n, err := fmt.Fprintf(w, format, args...)
		written += int64(n)
		return err
	}

	names := make([]string, 0, len(definitions))
	for name := range definitions {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		definition := definitions[name]
		if err := write("# HELP %s %s\n", name, definition.help); err != nil {
			return written, err
		}
		if err := write("# TYPE %s %s\n", name, definition.kind); err != nil {
			return written, err
		}

		switch definition.kind {
		case "counter":
			for _, sample := range snapshot.counters {
				if sample.name != name {
					continue
				}
				if err := write("%s%s %s\n", sample.name, formatLabels(sample.labels), formatFloat(sample.value)); err != nil {
					return written, err
				}
			}
		case "gauge":
			for _, sample := range snapshot.gauges {
				if sample.name != name {
					continue
				}
				if err := write("%s%s %s\n", sample.name, formatLabels(sample.labels), formatFloat(sample.value)); err != nil {
					return written, err
				}
			}
		case "histogram":
			for _, sample := range snapshot.histograms {
				if sample.name != name {
					continue
				}
				for i, bucket := range sample.buckets {
					labels := sample.labels.clone()
					labels["le"] = formatBucket(r.buckets[i])
					if err := write("%s_bucket%s %d\n", sample.name, formatLabels(labels), bucket); err != nil {
						return written, err
					}
				}
				labels := sample.labels.clone()
				labels["le"] = "+Inf"
				if err := write("%s_bucket%s %d\n", sample.name, formatLabels(labels), sample.count); err != nil {
					return written, err
				}
				if err := write("%s_sum%s %s\n", sample.name, formatLabels(sample.labels), formatFloat(sample.sum)); err != nil {
					return written, err
				}
				if err := write("%s_count%s %d\n", sample.name, formatLabels(sample.labels), sample.count); err != nil {
					return written, err
				}
			}
		}
	}

	return written, nil
}

type registrySnapshot struct {
	counters   []metricSample
	gauges     []metricSample
	histograms []histogramSnapshot
}

type metricSample struct {
	name   string
	labels metricLabels
	value  float64
}

type histogramSnapshot struct {
	name    string
	labels  metricLabels
	buckets []uint64
	count   uint64
	sum     float64
}

type metricLabels map[string]string

func (r *Registry) snapshotLocked() registrySnapshot {
	snapshot := registrySnapshot{
		counters:   make([]metricSample, 0, len(r.counters)),
		gauges:     make([]metricSample, 0, len(r.gauges)),
		histograms: make([]histogramSnapshot, 0, len(r.histograms)),
	}

	for key, value := range r.counters {
		name, labels := splitMetricKey(key)
		snapshot.counters = append(snapshot.counters, metricSample{name: name, labels: labels, value: value})
	}
	for key, value := range r.gauges {
		name, labels := splitMetricKey(key)
		snapshot.gauges = append(snapshot.gauges, metricSample{name: name, labels: labels, value: value})
	}
	for key, sample := range r.histograms {
		name, labels := splitMetricKey(key)
		buckets := make([]uint64, len(sample.buckets))
		copy(buckets, sample.buckets)
		snapshot.histograms = append(snapshot.histograms, histogramSnapshot{
			name:    name,
			labels:  labels,
			buckets: buckets,
			count:   sample.count,
			sum:     sample.sum,
		})
	}

	sortMetricSamples(snapshot.counters)
	sortMetricSamples(snapshot.gauges)
	sort.Slice(snapshot.histograms, func(i, j int) bool {
		left := snapshot.histograms[i]
		right := snapshot.histograms[j]
		if left.name != right.name {
			return left.name < right.name
		}
		return labelsKey(left.labels) < labelsKey(right.labels)
	})

	return snapshot
}

func sortMetricSamples(samples []metricSample) {
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].name != samples[j].name {
			return samples[i].name < samples[j].name
		}
		return labelsKey(samples[i].labels) < labelsKey(samples[j].labels)
	})
}

func metricKey(name string, labels Labels) string {
	metricLabels := make(metricLabels, len(labels))
	for key, value := range labels {
		metricLabels[key] = value
	}

	return name + "|" + labelsKey(metricLabels)
}

func splitMetricKey(key string) (string, metricLabels) {
	parts := strings.SplitN(key, "|", 2)
	name := parts[0]
	labels := metricLabels{}
	if len(parts) == 1 || parts[1] == "" {
		return name, labels
	}

	for _, pair := range strings.Split(parts[1], ",") {
		if pair == "" {
			continue
		}
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue
		}
		labels[keyValue[0]] = keyValue[1]
	}

	return name, labels
}

func labelsKey(labels metricLabels) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}

	return strings.Join(parts, ",")
}

func formatLabels(labels metricLabels) string {
	if len(labels) == 0 {
		return ""
	}

	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+`="`+escapeLabelValue(labels[key])+`"`)
	}

	return "{" + strings.Join(parts, ",") + "}"
}

func (l metricLabels) clone() metricLabels {
	clone := make(metricLabels, len(l))
	for key, value := range l {
		clone[key] = value
	}

	return clone
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)

	return value
}

func formatBucket(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func formatFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "0"
	}

	return strconv.FormatFloat(value, 'g', -1, 64)
}
