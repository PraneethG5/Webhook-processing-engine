package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/polyglot/webhook-gateway/internal/models"
	"github.com/redis/go-redis/v9"
)

const (
	StreamKey = "webhooks:stream"
)

// Producer handles pushing accepted webhooks into the Redis Stream
type Producer struct {
	client *redis.Client
}

// NewProducer initializes a new Redis Stream producer
func NewProducer(client *redis.Client) *Producer {
	return &Producer{
		client: client,
	}
}

// PushEvent adds the incoming webhook event to the Redis Stream
func (p *Producer) PushEvent(ctx context.Context, event *models.IngestEvent) (string, error) {
	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	values := map[string]interface{}{
		"event_id":        event.EventID,
		"tenant_id":       event.TenantID,
		"idempotency_key": event.IdempotencyKey,
		"event_type":      event.EventType,
		"payload":         string(payloadBytes),
		"received_at":     event.ReceivedAt,
		"max_retries":     event.MaxRetries,
	}

	streamID, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey,
		Values: values,
	}).Result()
	if err != nil {
		return "", fmt.Errorf("redis XAdd failed: %w", err)
	}

	return streamID, nil
}
