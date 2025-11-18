.PHONY: help build run test clean deps migrate-up migrate-down

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Download dependencies
	go mod download
	go mod tidy

build: ## Build the application
	go build -o bin/ai-validator ./cmd/validator

run: ## Run the application
	go run ./cmd/validator -config=config.yaml

test: ## Run tests
	go test -v -race -cover ./...

clean: ## Clean build artifacts
	rm -rf bin/
	rm -rf dist/

migrate-up: ## Run database migrations up
	migrate -path migrations -database "${DB_DSN}" up

migrate-down: ## Run database migrations down
	migrate -path migrations -database "${DB_DSN}" down

lint: ## Run linter
	golangci-lint run ./...

docker-build: ## Build Docker image
	docker build -t ai-validator:latest .
