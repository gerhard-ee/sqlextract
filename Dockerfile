# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache make git

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN make build

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/sqlextract .

# Create a non-root user
RUN adduser -D appuser
USER appuser

ENTRYPOINT ["./sqlextract"] 