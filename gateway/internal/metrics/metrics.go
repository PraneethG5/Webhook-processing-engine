package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total count of HTTP requests processed by endpoint and status code.",
		},
		[]string{"path", "status"},
	)

	WebhookAcceptedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_accepted_total",
			Help: "Total number of successfully authenticated and queued webhooks.",
		},
		[]string{"tenant_id"},
	)

	WebhookRejectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "webhook_rejected_total",
			Help: "Total number of rejected webhooks categorized by reason.",
		},
		[]string{"tenant_id", "reason"},
	)

	WebhookIngestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "webhook_ingest_duration_seconds",
			Help:    "Latency distribution of webhook ingestion from receipt to Redis enqueue.",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		},
		[]string{"tenant_id"},
	)

	RateLimitExceededTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_exceeded_total",
			Help: "Total number of requests dropped due to per-tenant token bucket exhaustion.",
		},
		[]string{"tenant_id"},
	)
)
