#!/bin/bash
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

PROD_DIR="$HOME/sites/whgazetteer-org"
PROD_BRANCH="main"
PROD_CONTAINER_PREFIX="whgazetteer-org_main"

DEV_DIR="$HOME/sites/dev-whgazetteer-org"
DEV_BRANCH="staging"
DEV_CONTAINER_PREFIX="dev-whgazetteer-org_staging"

COMPOSE="docker-compose -f docker-compose-autocontext.yml --env-file ./.env/.env"

# ─── Usage ───────────────────────────────────────────────────────────────────

usage() {
    cat <<EOF
Usage: deploy [ENV] [ACTION] [OPTIONS]

Environment (default: dev):
  dev         Deploy to development   ($DEV_DIR)
  prod        Deploy to production    ($PROD_DIR)

Action (default: restart):
  pull        Pull code only, no restart
  restart     Pull + regenerate config + restart web
  full        Pull + regenerate config + restart all containers
  recreate    Pull + regenerate config + tear down + recreate all
  status      Show running containers

Options:
  --branch=<name>  Override the dev branch (default: staging; prod always uses main)
  --celery    Also restart celery worker and beat (with 'restart')
  --migrate   Run Django migrations after deploy
  --logs      Tail web container logs after deploy

Examples:
  deploy                              # dev, restart web (staging branch)
  deploy prod                         # prod, restart web
  deploy dev pull                     # dev, pull only
  deploy prod full                    # prod, restart all
  deploy restart --celery             # dev, restart web + celery
  deploy prod recreate --migrate
  deploy --branch=api/crc-gateway     # dev, deploy a feature branch
  deploy pull --branch=api/crc-gateway  # dev, pull a feature branch only
EOF
    exit 0
}

# ─── Parse arguments ─────────────────────────────────────────────────────────

ENV="dev"
ACTION="restart"
BRANCH_OVERRIDE=""
WITH_CELERY=false
WITH_MIGRATE=false
WITH_LOGS=false

for arg in "$@"; do
    case "$arg" in
        dev|prod)     ENV="$arg" ;;
        pull|restart|full|recreate|status) ACTION="$arg" ;;
        --branch=*)   BRANCH_OVERRIDE="${arg#--branch=}" ;;
        --celery)     WITH_CELERY=true ;;
        --migrate)    WITH_MIGRATE=true ;;
        --logs)       WITH_LOGS=true ;;
        -h|--help)    usage ;;
        *)            echo "Unknown argument: $arg"; usage ;;
    esac
done

# ─── Set environment-specific variables ──────────────────────────────────────

if [ "$ENV" = "prod" ]; then
    if [ -n "$BRANCH_OVERRIDE" ]; then
        echo "Error: --branch is not allowed for production (always uses $PROD_BRANCH)."
        exit 1
    fi
    SITE_DIR="$PROD_DIR"
    BRANCH="$PROD_BRANCH"
    PREFIX="$PROD_CONTAINER_PREFIX"
else
    SITE_DIR="$DEV_DIR"
    BRANCH="${BRANCH_OVERRIDE:-$DEV_BRANCH}"
    PREFIX="$DEV_CONTAINER_PREFIX"
fi

WEB="web_${PREFIX}"
WORKER="celery-worker_${PREFIX}"
BEAT="celery-beat_${PREFIX}"

echo "═══ deploy $ENV ($ACTION) ═══"
echo "  Directory: $SITE_DIR"
echo "  Branch:    $BRANCH"
echo ""

cd "$SITE_DIR"

# ─── Status ──────────────────────────────────────────────────────────────────

if [ "$ACTION" = "status" ]; then
    docker ps --filter "name=${PREFIX}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    exit 0
fi

# ─── Pull ────────────────────────────────────────────────────────────────────

echo "── Pulling $BRANCH..."
git fetch origin
git checkout "$BRANCH" 2>/dev/null || true
git pull origin "$BRANCH" --rebase
echo ""

if [ "$ACTION" = "pull" ]; then
    echo "Done (pull only)."
    exit 0
fi

# ─── Regenerate config ───────────────────────────────────────────────────────

echo "── Regenerating config..."
sudo python3 ./server-admin/load_env.py
echo ""

# ─── Deploy action ───────────────────────────────────────────────────────────

# Check if any containers are running for this environment
RUNNING=$(docker ps --filter "name=${PREFIX}" --format "{{.Names}}" | head -1)

case "$ACTION" in
    restart|full)
        if [ -z "$RUNNING" ]; then
            echo "── No running containers found. Starting stack..."
            $COMPOSE up -d
        elif [ "$ACTION" = "full" ]; then
            echo "── Restarting all containers..."
            $COMPOSE restart
        else
            SERVICES="web"
            if [ "$WITH_CELERY" = true ]; then
                SERVICES="web celery_worker celery_beat"
            fi
            echo "── Restarting: $SERVICES"
            $COMPOSE restart $SERVICES
        fi
        ;;
    recreate)
        echo "── Recreating all containers..."
        $COMPOSE down
        $COMPOSE up -d
        ;;
esac
echo ""

# ─── Migrations ──────────────────────────────────────────────────────────────

if [ "$WITH_MIGRATE" = true ]; then
    echo "── Running migrations..."
    docker exec "$WEB" bash -c "./manage.py migrate"
    echo ""
fi

# ─── Status ──────────────────────────────────────────────────────────────────

echo "── Containers:"
docker ps --filter "name=${PREFIX}" --format "table {{.Names}}\t{{.Status}}"
echo ""

# ─── Logs ────────────────────────────────────────────────────────────────────

if [ "$WITH_LOGS" = true ]; then
    echo "── Tailing $WEB logs (Ctrl-C to stop)..."
    docker logs -f "$WEB"
fi

