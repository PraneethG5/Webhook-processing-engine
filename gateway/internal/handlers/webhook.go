package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/polyglot/webhook-gateway/internal/auth"
	"github.com/polyglot/webhook-gateway/internal/db"
	"github.com/polyglot/webhook-gateway/internal/metrics"
	"github.com/polyglot/webhook-gateway/internal/models"
	"github.com/polyglot/webhook-gateway/internal/queue"
	"github.com/polyglot/webhook-gateway/internal/ratelimit"
)

type WebhookHandler struct {
	tenantStore *db.TenantStore
	rateLimiter *ratelimit.TokenBucketLimiter
	producer    *queue.Producer
}

func NewWebhookHandler(ts *db.TenantStore, rl *ratelimit.TokenBucketLimiter, prod *queue.Producer) *WebhookHandler {
	return &WebhookHandler{
		tenantStore: ts,
		rateLimiter: rl,
		producer:    prod,
	}
}

func (h *WebhookHandler) HandleIngest(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		metrics.WebhookRejectedTotal.WithLabelValues("unknown", "missing_tenant_header").Inc()
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Tenant-ID header"})
		return
	}

	tenant, exists := h.tenantStore.GetTenant(tenantID)
	if !exists {
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "tenant_not_found").Inc()
		respondJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
	if err != nil {
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "body_read_error").Inc()
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	signature := r.Header.Get("X-Signature-SHA256")
	if signature == "" {
		signature = r.Header.Get("X-Signature")
	}

	if !auth.VerifyHMACSHA256(body, tenant.InboundSecret, signature) {
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "invalid_signature").Inc()
		respondJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid signature"})
		return
	}

	rlResult, err := h.rateLimiter.Allow(r.Context(), tenant.ID, tenant.RateLimitPerSecond, tenant.BurstLimit)
	if err != nil {
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "ratelimit_error").Inc()
	} else if !rlResult.Allowed {
		metrics.RateLimitExceededTotal.WithLabelValues(tenantID).Inc()
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "rate_limited").Inc()

		w.Header().Set("Retry-After", strconv.FormatInt(int64(rlResult.RetryAfter.Seconds())+1, 10))
		w.Header().Set("X-RateLimit-Remaining", "0")
		respondJSON(w, http.StatusTooManyRequests, map[string]interface{}{
			"error":            "rate limit exceeded",
			"retry_after_secs": rlResult.RetryAfter.Seconds(),
		})
		return
	}

	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(rlResult.RemainingTokens))

	eventID := uuid.New().String()
	idempotencyKey := r.Header.Get("X-Idempotency-Key")
	eventType := r.Header.Get("X-Event-Type")
	if eventType == "" {
		eventType = "webhook.event"
	}

	event := &models.IngestEvent{
		EventID:        eventID,
		TenantID:       tenant.ID,
		IdempotencyKey: idempotencyKey,
		EventType:      eventType,
		Payload:        json.RawMessage(body),
		ReceivedAt:     start.UnixMilli(),
		MaxRetries:     5,
	}

	if _, err := h.producer.PushEvent(r.Context(), event); err != nil {
		metrics.WebhookRejectedTotal.WithLabelValues(tenantID, "queue_push_error").Inc()
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to enqueue event"})
		return
	}

	go func(ev *models.IngestEvent) {
		_ = h.tenantStore.RecordInitialEvent(context.Background(), ev)
	}(event)

	duration := time.Since(start).Seconds()
	metrics.WebhookIngestDuration.WithLabelValues(tenantID).Observe(duration)
	metrics.WebhookAcceptedTotal.WithLabelValues(tenantID).Inc()

	respondJSON(w, http.StatusAccepted, models.WebhookResponse{
		Status:    "accepted",
		EventID:   eventID,
		Message:   "queued",
		Timestamp: time.Now().UnixMilli(),
	})
}

func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}
