#!/bin/bash
set -e  # Exit on any error

# Color output for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Change to the project directory
cd ~/sites/whgazetteer-org

echo -e "${BLUE}==> Pulling latest changes from git...${NC}"
git pull origin main

echo -e "${BLUE}==> Loading environment variables...${NC}"
sudo python3 ./server-admin/load_env.py

# Check if webpack build is requested
if [[ "$1" == "-webpack" ]] || [[ "$1" == "--webpack" ]]; then
    echo -e "${BLUE}==> Running webpack build...${NC}"
    docker compose -f webpack.build.yml -p whgazetteer-org-build run --rm webpack-builder
fi

echo -e "${BLUE}==> Restarting services...${NC}"
docker compose -f docker-compose-autocontext.yml --env-file ./.env/.env up -d --force-recreate

echo -e "${BLUE}==> Current running containers:${NC}"
docker ps

echo -e "${GREEN}==> Deployment complete!${NC}"