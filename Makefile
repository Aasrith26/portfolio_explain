.PHONY: build start stop restart logs test clean dev prod

# Default environment
ENV ?= dev

# Build the Docker image
build:
	docker-compose build

# Start services
start:
	docker-compose up -d

# Stop services
stop:
	docker-compose down

# Restart services
restart: stop start

# View logs
logs:
	docker-compose logs -f

# Run tests
test:
	docker-compose exec portfolio-explainer python -m pytest tests/ -v

# Clean up containers and images
clean:
	docker-compose down -v --remove-orphans
	docker system prune -f

# Development environment
dev:
	docker-compose -f docker-compose.dev.yml up -d

# Production environment with nginx
prod:
	docker-compose --profile production up -d

# Health check
health:
	curl -f http://localhost:5001/ || echo "API not responding"

# Full deployment (production ready)
deploy:
	./scripts/deploy.sh

# Development setup
dev-setup:
	./scripts/dev.sh
