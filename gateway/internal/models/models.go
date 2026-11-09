package models

import (
	"encoding/json"
	"time"
)

// Tenant represents tenant security and rate limiting config
type Tenant struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	InboundSecret      string    `json:"inbound_secret"`
	OutboundSecret     string    `json:"outbound_secret"`
	DestinationURL     string    `json:"destination_url"`
	RateLimitPerSecond int       `json:"rate_limit_per_second"`
	BurstLimit         int       `json:"burst_limit"`
	CreatedAt          time.Time `json:"created_at"`
}

// IngestEvent represents the standardized event payload pushed into Redis Streams
type IngestEvent struct {
	EventID        string          `json:"event_id"`
	TenantID       string          `json:"tenant_id"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	ReceivedAt     int64           `json:"received_at"` // Unix millisecond timestamp
	MaxRetries     int             `json:"max_retries"`
}

// WebhookResponse is the fast JSON response returned to the caller
type WebhookResponse struct {
	Status    string `json:"status"`
	EventID   string `json:"event_id"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}
