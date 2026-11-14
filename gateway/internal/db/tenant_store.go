package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/polyglot/webhook-gateway/internal/models"
)

type TenantStore struct {
	db      *sql.DB
	rwMutex sync.RWMutex
	cache   map[string]*models.Tenant
}

func NewTenantStore(dbURL string) (*TenantStore, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to open db: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	store := &TenantStore{
		db:    db,
		cache: make(map[string]*models.Tenant),
	}

	for i := 0; i < 10; i++ {
		if err := store.refreshCache(); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	go store.startSyncLoop(30 * time.Second)

	return store, nil
}

func (s *TenantStore) startSyncLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if err := s.refreshCache(); err != nil {
			log.Printf("tenant store sync error: %v", err)
		}
	}
}

func (s *TenantStore) refreshCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, inbound_secret, outbound_secret, destination_url, rate_limit_per_second, burst_limit, created_at
		FROM tenants
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	newCache := make(map[string]*models.Tenant)
	for rows.Next() {
		var t models.Tenant
		if err := rows.Scan(
			&t.ID,
			&t.Name,
			&t.InboundSecret,
			&t.OutboundSecret,
			&t.DestinationURL,
			&t.RateLimitPerSecond,
			&t.BurstLimit,
			&t.CreatedAt,
		); err != nil {
			return err
		}
		newCache[t.ID] = &t
	}

	s.rwMutex.Lock()
	s.cache = newCache
	s.rwMutex.Unlock()

	return nil
}

func (s *TenantStore) GetTenant(tenantID string) (*models.Tenant, bool) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	t, ok := s.cache[tenantID]
	return t, ok
}

func (s *TenantStore) RecordInitialEvent(ctx context.Context, event *models.IngestEvent) error {
	query := `
		INSERT INTO webhook_events (id, tenant_id, idempotency_key, event_type, payload, status, max_retries, created_at)
		VALUES ($1, $2, $3, $4, $5, 'PENDING', $6, NOW())
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
	`
	_, err := s.db.ExecContext(ctx, query,
		event.EventID,
		event.TenantID,
		sql.NullString{String: event.IdempotencyKey, Valid: event.IdempotencyKey != ""},
		event.EventType,
		string(event.Payload),
		event.MaxRetries,
	)
	return err
}
