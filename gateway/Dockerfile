# Multi-stage Dockerfile for Go Gateway
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git and ca-certificates
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Compile static binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /app/gateway ./cmd/main.go

# Production minimal runner
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/gateway /app/gateway

EXPOSE 8080

USER 1000:1000

ENTRYPOINT ["/app/gateway"]
