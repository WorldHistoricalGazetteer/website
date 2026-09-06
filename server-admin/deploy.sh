#!/bin/bash
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

PROD_DIR="$HOME/sites/whgazetteer-org"
PROD_BRANCH="main"
PROD_ENV_CONTEXT="whgazetteer-org"

DEV_DIR="$HOME/sites/dev-whgazetteer-org"
DEV_BRANCH="staging"
DEV_ENV_CONTEXT="dev-whgazetteer-org"

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
  --image=<tag>    Point this site at a different Docker image tag before deploying.
                   Needed after a requirements.txt change: build_docker.py pushes the
                   image, this moves the site onto it. Edits DOCKER_IMAGE_TAG in
                   /home/whgadmin/sites/env_template.py (backed up first).
  --celery    Also restart celery worker and beat (with 'restart')
  --migrate   Run Django migrations after deploy
  --collectstatic  Run Django collectstatic after deploy
  --logs      Tail web container logs after deploy

Examples:
  deploy                              # dev, restart web (staging branch)
  deploy prod                         # prod, restart web
  deploy dev pull                     # dev, pull only
  deploy prod full                    # prod, restart all
  deploy restart --celery             # dev, restart web + celery
  deploy prod recreate --migrate
  deploy prod recreate --collectstatic  # prod, recreate + collect static files
  deploy --image=1.0.19 restart       # dev, move onto a freshly built image
  deploy --branch=api/crc-gateway     # dev, deploy a feature branch
  deploy pull --branch=api/crc-gateway  # dev, pull a feature branch only
EOF
    exit 0
}

# ─── Parse arguments ─────────────────────────────────────────────────────────

ENV="dev"
ACTION="restart"
BRANCH_OVERRIDE=""
IMAGE_TAG=""
WITH_CELERY=false
WITH_MIGRATE=false
WITH_COLLECTSTATIC=false
WITH_LOGS=false

for arg in "$@"; do
    case "$arg" in
        dev|prod)     ENV="$arg" ;;
        pull|restart|full|recreate|status) ACTION="$arg" ;;
        --branch=*)   BRANCH_OVERRIDE="${arg#--branch=}" ;;
        --image=*)    IMAGE_TAG="${arg#--image=}" ;;
        --celery)     WITH_CELERY=true ;;
        --migrate)    WITH_MIGRATE=true ;;
        --collectstatic) WITH_COLLECTSTATIC=true ;;
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
    PREFIX="${PROD_ENV_CONTEXT}_${BRANCH}"
else
    SITE_DIR="$DEV_DIR"
    BRANCH="${BRANCH_OVERRIDE:-$DEV_BRANCH}"
    # Match load_env.py branch normalization used by docker-compose template.
    BRANCH_SAFE="${BRANCH//\//--}"
    PREFIX="${DEV_ENV_CONTEXT}_${BRANCH_SAFE}"
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

echo "── Fetching origin..."
git fetch origin

CURRENT=$(git branch --show-current 2>/dev/null || echo "")
if [ "$CURRENT" != "$BRANCH" ]; then
    echo "── Switching from ${CURRENT:-detached} to $BRANCH..."
    # Clean untracked files that would block the checkout (e.g. stale webpack bundles)
    git clean -fd -- static/webpack/ 2>/dev/null || true
    git checkout -f "$BRANCH"
fi

echo "── Resetting to origin/$BRANCH..."
git reset --hard "origin/$BRANCH"
echo ""

if [ "$ACTION" = "pull" ]; then
    echo "Done (pull only)."
    exit 0
fi

# ─── Regenerate config ───────────────────────────────────────────────────────

# The image tag lives in the server's env_template.py, which is not in the repo.
# Bumping it here rather than by hand is the point: a pushed image whose tag never
# moved leaves the site running the old one, with nothing to say so.
if [ -n "$IMAGE_TAG" ]; then
    echo "── Setting image tag to $IMAGE_TAG for $ENV_CONTEXT..."
    sudo python3 ./server-admin/set_image_tag.py --site "$ENV_CONTEXT" --tag "$IMAGE_TAG"
    echo ""
fi

echo "── Regenerating config..."
sudo python3 ./server-admin/load_env.py
echo ""

# ─── Shared LLM network ──────────────────────────────────────────────────────

# Both stacks attach their `ner` container to this bridge so they can share the single `ollama`
# instance that the production stack runs (place#211). It is declared `external` in both compose
# files precisely so that neither stack's `down` can remove it while the other is using it — which
# means nothing else creates it. Idempotent.
LLM_NETWORK=$(grep -E '^OLLAMA_NETWORK=' ./.env/.env 2>/dev/null | cut -d= -f2- || true)
if [ -n "${LLM_NETWORK:-}" ]; then
    if ! docker network inspect "$LLM_NETWORK" >/dev/null 2>&1; then
        echo "── Creating shared LLM network $LLM_NETWORK..."
        docker network create "$LLM_NETWORK"
    fi
fi
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

# ─── Collect static ──────────────────────────────────────────────────────────

if [ "$WITH_COLLECTSTATIC" = true ]; then
    echo "── Collecting static files..."
    docker exec "$WEB" bash -c "./manage.py collectstatic --noinput"
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

