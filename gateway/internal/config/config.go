package config

import (
	"os"
)

type Config struct {
	Port          string
	RedisAddr     string
	RedisPassword string
	DatabaseURL   string
}

func LoadConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@postgres:5432/webhooks?sslmode=disable"
	}

	return &Config{
		Port:          port,
		RedisAddr:     redisAddr,
		RedisPassword: redisPassword,
		DatabaseURL:   dbURL,
	}
}
