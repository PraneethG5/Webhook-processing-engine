CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    inbound_secret VARCHAR(255) NOT NULL,
    outbound_secret VARCHAR(255) NOT NULL,
    destination_url TEXT NOT NULL,
    rate_limit_per_second INT NOT NULL DEFAULT 50,
    burst_limit INT NOT NULL DEFAULT 100,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS webhook_events (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(255),
    event_type VARCHAR(100) NOT NULL DEFAULT 'webhook.event',
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    max_retries INT NOT NULL DEFAULT 5,
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    delivered_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT uq_tenant_idempotency UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    destination_url TEXT NOT NULL,
    request_headers JSONB,
    status_code INT,
    response_body TEXT,
    error_message TEXT,
    latency_ms INT,
    attempted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE REFERENCES webhook_events(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    failure_reason TEXT NOT NULL,
    total_attempts INT NOT NULL,
    last_error TEXT,
    replayed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_tenant_status ON webhook_events(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON webhook_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_attempts_event_id ON delivery_attempts(event_id);
CREATE INDEX IF NOT EXISTS idx_dlq_tenant ON dead_letter_events(tenant_id, replayed_at);

INSERT INTO tenants (id, name, inbound_secret, outbound_secret, destination_url, rate_limit_per_second, burst_limit)
VALUES 
    ('11111111-1111-1111-1111-111111111111', 'tenant-alpha', 'sec_inbound_alpha_9f8e7d', 'sec_outbound_alpha_1a2b3c', 'http://mock-destination:9000/webhook', 100, 200),
    ('22222222-2222-2222-2222-222222222222', 'tenant-beta', 'sec_inbound_beta_4b5c6d', 'sec_outbound_beta_7e8f9a', 'http://mock-destination:9000/webhook', 20, 40)
ON CONFLICT (id) DO NOTHING;
