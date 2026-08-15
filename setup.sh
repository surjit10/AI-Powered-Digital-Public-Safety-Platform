#!/usr/bin/env bash
# ==============================================================================
# AI Cyber Fraud Detection Platform — 1-Click Setup Script (Linux / macOS)
# ==============================================================================
set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

echo -e "${CYAN}=============================================${NC}"
echo -e "${CYAN}  AI Fraud Detection Platform - Setup (Bash) ${NC}"
echo -e "${CYAN}=============================================${NC}"

# Detect Python command
PYTHON_CMD="python3"
if ! command -v python3 &> /dev/null; then
    if command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        echo -e "${RED}Error: Python 3 is required but not found in PATH.${NC}"
        exit 1
    fi
fi

# 1. Environment variables
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}[1/5] Creating .env from .env.example...${NC}"
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo -e "${GREEN}Created .env file.${NC}"
    else
        echo -e "${RED}Warning: .env.example not found, creating empty .env file.${NC}"
        touch .env
    fi
else
    echo -e "${GREEN}[1/5] .env already exists. Skipping.${NC}"
fi

# 2. Generate RSA Keypair & Configure Kong
echo -e "${YELLOW}[2/5] Generating fresh RSA keys for JWT signing & configuring Kong...${NC}"
$PYTHON_CMD generate_keys.py
$PYTHON_CMD add_kong_consumers.py

# 3. Docker Compose Up
echo -e "${YELLOW}[3/5] Starting infrastructure via Docker Compose (building containers)...${NC}"
docker compose up -d

echo -e "${YELLOW}Waiting for all containers to become healthy (timeout: 10 minutes)...${NC}"
MAX_WAIT_SECONDS=600
START_TIME=$(date +%s)

no_healthcheck_containers=("platform-outbox-publisher" "platform-search-consumer" "platform-kafka-init" "platform-minio-init")

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    if [ $ELAPSED -ge $MAX_WAIT_SECONDS ]; then
        echo -e "${RED}Warning: Timed out waiting for containers. Check status with 'docker compose ps'.${NC}"
        break
    fi

    # Inspect JSON status from docker compose ps
    STATUS_JSON=$(docker compose ps --format json 2>/dev/null || true)
    
    if [ -z "$STATUS_JSON" ]; then
        echo -e "${GRAY}Waiting for Docker Compose to initialize containers...${NC}"
        sleep 5
        continue
    fi

    # Check if there are unhealthy/not running containers
    ALL_READY=true
    
    # We parse container statuses using python
    UNHEALTHY=$($PYTHON_CMD -c '
import sys, json

data = sys.stdin.read().strip()
if not data:
    print("ALL_EMPTY")
    sys.exit(0)

# Docker compose ps --format json can return a list or newline-delimited objects
containers = []
try:
    parsed = json.loads(data)
    if isinstance(parsed, list):
        containers = parsed
    else:
        containers = [parsed]
except:
    for line in data.splitlines():
        line = line.strip()
        if line:
            try:
                containers.append(json.loads(line))
            except:
                pass

no_hc = ["platform-outbox-publisher", "platform-search-consumer", "platform-kafka-init", "platform-minio-init"]
unhealthy_names = []

for c in containers:
    name = c.get("Name") or c.get("Service", "")
    state = (c.get("State") or "").lower()
    health = (c.get("Health") or "").lower()
    
    if any(nhc in name for nhc in no_hc):
        if state != "running":
            unhealthy_names.append(name)
    elif health:
        if health != "healthy":
            unhealthy_names.append(name)
    else:
        if state != "running":
            unhealthy_names.append(name)

print(", ".join(unhealthy_names))
' <<< "$STATUS_JSON")

    if [ -z "$UNHEALTHY" ]; then
        echo -e "${GREEN}All containers are running and healthy!${NC}"
        break
    else
        echo -e "${GRAY}Still waiting for: ${UNHEALTHY}${NC}"
        sleep 5
    fi
done

# 4. Provision Kafka Topics
echo -e "${YELLOW}[4/5] Provisioning Kafka Topics...${NC}"
docker compose exec kafka /bin/bash /infra/kafka/provision-topics.sh || echo -e "${YELLOW}Kafka topic script finished.${NC}"

# OpenSearch Cluster Health Check
echo -e "${GRAY}Checking OpenSearch cluster readiness...${NC}"
for i in {1..5}; do
    if curl -s -f "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=30s" > /dev/null 2>&1; then
        echo -e "${GREEN}OpenSearch is ready.${NC}"
        break
    else
        echo -e "${GRAY}OpenSearch not ready yet, retrying... ($i/5)${NC}"
        sleep 10
    fi
done

# 5. Provision Demo Accounts in Auth Service
echo -e "${YELLOW}[5/5] Provisioning Demo Accounts in Auth Service...${NC}"
$PYTHON_CMD create_demo_accounts.py || echo -e "${RED}Warning: Could not provision demo accounts automatically. Run '$PYTHON_CMD create_demo_accounts.py' manually if needed.${NC}"

echo -e "${CYAN}=============================================${NC}"
echo -e "${GREEN}Setup Complete! Platform is Ready.${NC}"
echo -e "You can now access:"
echo -e "  ${YELLOW}Citizen Portal:${NC}          http://localhost:5173"
echo -e "  ${YELLOW}Telecom Admin Portal:${NC}    http://localhost:5174"
echo -e "  ${YELLOW}Bank Fraud Monitor:${NC}      http://localhost:5175"
echo -e "  ${YELLOW}MHA / Gov Dashboard:${NC}     http://localhost:5176"
echo -e "  ${YELLOW}Investigator Portal:${NC}     http://localhost:5177"
echo -e "  ${YELLOW}Grafana (Observability):${NC} http://localhost:3000  (admin / admin)"
echo -e "  ${YELLOW}Kong API Gateway:${NC}        http://localhost:8000"
echo -e "  ${YELLOW}Kong Admin API:${NC}          http://localhost:8001"
echo -e "  ${YELLOW}MinIO Console (S3):${NC}      http://localhost:9001  (minioadmin / change_me_minio)"
echo -e "  ${YELLOW}Neo4j Graph Browser:${NC}     http://localhost:7474  (neo4j / change_me_neo4j)"
echo -e "  ${YELLOW}OpenSearch API:${NC}          http://localhost:9200"
echo -e "${CYAN}=============================================${NC}"
