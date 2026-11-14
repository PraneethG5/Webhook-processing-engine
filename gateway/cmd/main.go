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
	cfg := config.LoadConfig()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("redis ping failed: %v", err)
	}

	rateLimiter, err := ratelimit.NewTokenBucketLimiter(context.Background(), rdb)
	if err != nil {
		log.Fatalf("failed to init rate limiter: %v", err)
	}

	tenantStore, err := db.NewTenantStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to init tenant store: %v", err)
	}

	producer := queue.NewProducer(rdb)
	webhookHandler := handlers.NewWebhookHandler(tenantStore, rateLimiter, producer)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/webhooks", webhookHandler.HandleIngest)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("gateway listening on :%s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down gateway...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown error: %v", err)
	}

	_ = rdb.Close()
}
