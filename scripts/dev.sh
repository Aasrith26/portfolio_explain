#!/bin/bash

# Development environment startup script

set -e

echo "🔧 Starting Portfolio Explainer API - Development Mode"
echo "===================================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env from example..."
    cp .env.example .env
    echo "⚠️ Please edit .env with your actual credentials before continuing"
    exit 1
fi

# Build and start development environment
echo "Building development container..."
docker-compose -f docker-compose.dev.yml build

echo "Starting development services..."
docker-compose -f docker-compose.dev.yml up -d

echo ""
echo "🎉 Development environment is running!"
echo "   API: http://localhost:5001/"
echo "   Auto-reload enabled for code changes"
echo ""
echo "📋 Development commands:"
echo "   View logs: docker-compose -f docker-compose.dev.yml logs -f"
echo "   Stop: docker-compose -f docker-compose.dev.yml down"
echo "   Shell access: docker-compose -f docker-compose.dev.yml exec portfolio-explainer-dev bash"
