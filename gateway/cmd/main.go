package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/polyglot/webhook-gateway/internal/config"
	"github.com/polyglot/webhook-gateway/internal/db"
	"github.com/polyglot/webhook-gateway/internal/handlers"
	"github.com/polyglot/webhook-gateway/internal/queue"
	"github.com/polyglot/webhook-gateway/internal/ratelimit"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("[Gateway] Starting Polyglot Webhook Ingestion Engine...")

	cfg := config.LoadConfig()

	// 1. Redis Connection
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("[Gateway] Warning: Redis ping failed (%v), will retry on operations", err)
	} else {
		log.Println("[Gateway] Connected to Redis successfully.")
	}

	// 2. Token Bucket Rate Limiter
	rateLimiter, err := ratelimit.NewTokenBucketLimiter(context.Background(), rdb)
	if err != nil {
		log.Fatalf("[Gateway] Failed to initialize Redis token bucket limiter: %v", err)
	}

	// 3. PostgreSQL Tenant Store
	tenantStore, err := db.NewTenantStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[Gateway] Failed to initialize tenant store: %v", err)
	}

	// 4. Redis Stream Producer
	producer := queue.NewProducer(rdb)

	// 5. HTTP Handlers & Router
	webhookHandler := handlers.NewWebhookHandler(tenantStore, rateLimiter, producer)

	mux := http.NewServeMux()

	// Webhook Ingest Endpoint
	mux.HandleFunc("/v1/webhooks", webhookHandler.HandleIngest)

	// Health Check Endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"go-webhook-gateway"}`))
	})

	// Prometheus Metrics Endpoint
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful Shutdown Channel
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[Gateway] Listening and serving on port %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Gateway] Server error: %v", err)
		}
	}()

	<-stop
	log.Println("[Gateway] Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("[Gateway] Graceful shutdown failed: %v", err)
	}

	_ = rdb.Close()
	log.Println("[Gateway] Server stopped clean.")
}
