#!/bin/bash
set -e  # Exit on any error

# Color output for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Help message
show_help() {
    echo -e "${YELLOW}Usage: deploy [OPTION]${NC}"
    echo ""
    echo "Deploy the WHG application with various options."
    echo ""
    echo "Options:"
    echo "  (no option)        Deploy without webpack build (git pull + restart services)"
    echo "  -webpack           Deploy with webpack build (git pull + webpack + restart services)"
    echo "  -webpackonly       Only run webpack build (no git pull, no restart)"
    echo "  -help, --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  deploy              # Quick deploy without webpack"
    echo "  deploy -webpack     # Full deploy with webpack rebuild"
    echo "  deploy -webpackonly # Just rebuild webpack bundles"
    exit 0
}

# Check for help flag
if [[ "$1" == "-help" ]] || [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    show_help
fi

# Change to the project directory
cd ~/sites/whgazetteer-org

# Check if webpack-only mode
if [[ "$1" == "-webpackonly" ]] || [[ "$1" == "--webpackonly" ]]; then
    echo -e "${BLUE}==> Running webpack build only...${NC}"
    docker compose -f webpack.build.yml -p whgazetteer-org-build run --rm webpack-builder
    echo -e "${GREEN}==> Webpack build complete!${NC}"
    exit 0
fi

echo -e "${BLUE}==> Pulling latest changes from git...${NC}"
git pull --rebase origin main

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