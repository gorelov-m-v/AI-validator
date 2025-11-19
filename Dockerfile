# Build stage
FROM golang:1.25.4-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ai-validator ./cmd/validator

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/ai-validator .

# Copy migrations (optional, if you want to run them from container)
COPY --from=builder /app/migrations ./migrations

# Run the application
ENTRYPOINT ["./ai-validator"]
CMD ["-config=/config/config.yaml"]
