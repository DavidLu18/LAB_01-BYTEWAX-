#!/bin/bash

# Complete OHLCV Data Pipeline Startup Script
# Author: David
# Description: Manages the 3-step OHLCV data pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}🚀 OHLCV Data Pipeline Manager${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to display pipeline steps
show_pipeline() {
    echo -e "\n${YELLOW}📊 Pipeline Architecture:${NC}"
    echo -e "STEP 0: ${GREEN}Binance WebSocket${NC} → Kafka topic '${YELLOW}ohlcv.raw${NC}'"
    echo -e "STEP 1: ${GREEN}Transform${NC} '${YELLOW}ohlcv.raw${NC}' → '${YELLOW}ohlcv.processed${NC}'"
    echo -e "STEP 2: ${GREEN}Sink${NC} '${YELLOW}ohlcv.processed${NC}' → ${YELLOW}PostgreSQL${NC}"
}

# Function to check requirements
check_requirements() {
    echo -e "\n${BLUE}🔍 Checking requirements...${NC}"
    
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}❌ Docker Compose is not installed${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ All requirements satisfied${NC}"
}

# Function to start the pipeline
start_pipeline() {
    echo -e "\n${BLUE}🚀 Starting OHLCV Pipeline...${NC}"
    
    # Pull latest images
    echo -e "${YELLOW}📦 Pulling Docker images...${NC}"
    docker-compose pull
    
    # Build application images
    echo -e "${YELLOW}🔧 Building application...${NC}"
    docker-compose build
    
    # Start infrastructure first (PostgreSQL, Kafka)
    echo -e "${YELLOW}🗄️ Starting infrastructure...${NC}"
    docker-compose up -d postgres redpanda
    
    # Wait for services to be healthy
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 30
    
    # Start pipeline services
    echo -e "${YELLOW}📊 Starting pipeline services...${NC}"
    docker-compose up -d
    
    echo -e "\n${GREEN}✅ Pipeline started successfully!${NC}"
    show_status
}

# Function to show status
show_status() {
    echo -e "\n${BLUE}📊 Service Status:${NC}"
    docker-compose ps
    
    echo -e "\n${BLUE}🌐 Access Points:${NC}"
    echo -e "• PostgreSQL: ${YELLOW}localhost:5432${NC} (user: david, db: DavidDB)"
    echo -e "• Kafka: ${YELLOW}localhost:19092${NC}"
    echo -e "• Redpanda Console: ${YELLOW}http://localhost:8080${NC}"
    
    echo -e "\n${BLUE}📋 Useful Commands:${NC}"
    echo -e "• View logs: ${YELLOW}docker-compose logs -f [service_name]${NC}"
    echo -e "• Check DB: ${YELLOW}docker exec -it postgres_db psql -U david -d DavidDB${NC}"
    echo -e "• Stop pipeline: ${YELLOW}./start_pipeline.sh stop${NC}"
}

# Function to stop the pipeline
stop_pipeline() {
    echo -e "\n${RED}🛑 Stopping OHLCV Pipeline...${NC}"
    docker-compose down
    echo -e "${GREEN}✅ Pipeline stopped${NC}"
}

# Function to view logs
view_logs() {
    echo -e "\n${BLUE}📋 Available services for logs:${NC}"
    echo -e "• ${YELLOW}step0_binance_ws${NC} - Binance WebSocket producer"
    echo -e "• ${YELLOW}step1_transform${NC} - OHLC transformer"
    echo -e "• ${YELLOW}step2_sink${NC} - PostgreSQL sink"
    echo -e "• ${YELLOW}postgres${NC} - Database"
    echo -e "• ${YELLOW}redpanda${NC} - Message broker"
    
    read -p "Enter service name (or 'all' for all services): " service
    
    if [ "$service" = "all" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f "$service"
    fi
}

# Function to clean up
cleanup() {
    echo -e "\n${RED}🧹 Cleaning up (removes volumes and data)...${NC}"
    read -p "Are you sure? This will delete all data (y/N): " confirm
    
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        docker-compose down -v
        docker system prune -f
        echo -e "${GREEN}✅ Cleanup completed${NC}"
    else
        echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
    fi
}

# Main menu
main_menu() {
    show_pipeline
    echo -e "\n${BLUE}📋 Available Commands:${NC}"
    echo -e "1. ${GREEN}start${NC}   - Start the complete pipeline"
    echo -e "2. ${YELLOW}status${NC}  - Show pipeline status"
    echo -e "3. ${BLUE}logs${NC}    - View service logs"
    echo -e "4. ${RED}stop${NC}    - Stop the pipeline"
    echo -e "5. ${RED}cleanup${NC} - Clean up (removes data)"
    echo -e "6. ${GREEN}help${NC}    - Show this menu"
}

# Handle command line arguments
case "${1:-help}" in
    "start")
        check_requirements
        start_pipeline
        ;;
    "stop")
        stop_pipeline
        ;;
    "status")
        show_status
        ;;
    "logs")
        view_logs
        ;;
    "cleanup")
        cleanup
        ;;
    "help")
        main_menu
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        main_menu
        ;;
esac 