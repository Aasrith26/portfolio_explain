#!/bin/bash

# Portfolio Explainer API - Docker Deployment Script

set -e

echo "🐳 Portfolio Explainer API - Docker Deployment"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if .env file exists
if [ ! -f .env ]; then
    print_error ".env file not found!"
    echo "Please copy .env.example to .env and fill in your credentials:"
    echo "  cp .env.example .env"
    exit 1
fi

# Validate required environment variables
echo "Checking environment variables..."
required_vars=("AZURE_OPENAI_KEY" "AZURE_OPENAI_ENDPOINT" "AZURE_OPENAI_DEPLOYMENT")
missing_vars=()

for var in "${required_vars[@]}"; do
    if ! grep -q "^${var}=" .env; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    print_error "Missing required environment variables:"
    printf '  %s\n' "${missing_vars[@]}"
    echo "Please add them to your .env file"
    exit 1
fi

print_status "Environment variables validated"

# Build and deploy
echo ""
echo "Building Docker images..."
docker-compose build

print_status "Docker images built successfully"

echo ""
echo "Starting services..."
docker-compose up -d

print_status "Services started"

# Wait for health check
echo ""
echo "Waiting for API to be ready..."
timeout=60
counter=0

while [ $counter -lt $timeout ]; do
    if curl -s -f http://localhost:5001/ > /dev/null 2>&1; then
        print_status "API is ready!"
        break
    fi

    echo -n "."
    sleep 2
    counter=$((counter + 2))
done

if [ $counter -ge $timeout ]; then
    print_error "API failed to start within $timeout seconds"
    echo ""
    echo "Check logs with: docker-compose logs"
    exit 1
fi

echo ""
print_status "Deployment completed successfully!"
echo ""
echo "🌟 Your Portfolio Explainer API is now running:"
echo "   Health Check: http://localhost:5001/"
echo "   Component Test: http://localhost:5001/test-components"
echo ""
echo "📋 Useful commands:"
echo "   View logs: docker-compose logs -f"
echo "   Stop services: docker-compose down"
echo "   Restart: docker-compose restart"
echo "   Update: ./scripts/deploy.sh"
