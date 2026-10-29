#!/bin/bash
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

PROD_DIR="$HOME/sites/whgazetteer-org"
PROD_BRANCH="main"
PROD_ENV_CONTEXT="whgazetteer-org"

DEV_DIR="$HOME/sites/dev-whgazetteer-org"
DEV_BRANCH="staging"
DEV_ENV_CONTEXT="dev-whgazetteer-org"

COMPOSE_ARGS="-f docker-compose-autocontext.yml --env-file ./.env/.env"

# The image repo whose services --image= moves; anything else (postgres, redis,
# hocuspocus, ollama) is infrastructure and must not be recreated with it.
WHG_IMAGE="worldhistoricalgazetteer/web"

# ─── Host differences ────────────────────────────────────────────────────────
#
# These are functions, defined before main(), so they can be tested on their own (see
# server-admin/tests). Each one preserves what the current servers do and only does something
# different where the host is genuinely different, so one script serves every host.

# Which compose front end to use. The current servers have the standalone `docker-compose`, and
# keep using it. A host with only the Docker CE plugin (`docker compose`) gets that. WHG_COMPOSE
# overrides both.
pick_compose() {
    if [ -n "${WHG_COMPOSE:-}" ]; then
        echo "$WHG_COMPOSE"
    elif command -v docker-compose >/dev/null 2>&1; then
        echo "docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    else
        echo "docker-compose"   # not found: let the familiar error be the one that appears
    fi
}

# Whether the config renderer and image-tag editor run under sudo. They need it only because the
# current servers keep their nginx file in /etc/nginx and their env_template.py root-owned. A host
# that deploys without root (WHG_NO_SUDO=1, or no sudo installed) writes to directories its deploy
# user owns instead; see WHG_NGINX_DIR and WHG_SITES_DIR in load_env.py.
pick_sudo() {
    [ "${WHG_NO_SUDO:-0}" = "1" ] && return 0
    command -v sudo 2>/dev/null || true
}

# Run one of the server-admin Python scripts, under sudo where this host uses it. The overrides are
# passed through explicitly because sudo does not keep the environment.
run_admin_py() {
    local script="$1"; shift
    # shellcheck disable=SC2086  # SUDO is either empty or the word "sudo"
    $SUDO env WHG_SITES_DIR="${WHG_SITES_DIR:-}" WHG_DATABASES_DIR="${WHG_DATABASES_DIR:-}" \
        WHG_NGINX_DIR="${WHG_NGINX_DIR:-}" python3 "$script" "$@"
}

# The compose services of THIS site whose running container uses the WHG image, one per line.
# Derived from what is running, not hand-listed: a hardcoded list goes stale when a service is
# added, and mapping service names to container names by hand gets it wrong (prod's `flower`
# service is `celery-flower_<prefix>`). It reads the compose service label off `docker ps` rather
# than `compose ps -q`, because podman-compose has no `ps -q`; both Docker's compose and
# podman-compose label containers with com.docker.compose.service. $1 = container-name prefix.
whg_image_services() {
    docker ps --filter "name=$1" --format '{{.Image}}|{{.Label "com.docker.compose.service"}}' \
        | awk -F'|' -v img="$WHG_IMAGE" '$2 != "" && ($1 == img || index($1, img ":") == 1) { print $2 }' \
        | sort -u
}

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
                   /home/whgadmin/sites/env_template.py (backed up first), then
                   recreates ONLY the running services whose container actually
                   runs the WHG image (asked of compose, not hardcoded) with
                   --no-deps. Postgres, redis, hocuspocus and ollama are never
                   touched. Implies the recreate a
                   plain restart cannot do, so --celery is redundant with it.
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

# ─── Body ────────────────────────────────────────────────────────────────────
#
# Everything below runs inside main(). That is not style: this script does
# `git reset --hard` on the very checkout it is being read from, and bash reads a
# script incrementally by byte offset. Replace the file mid-run and execution
# resumes at an offset that now lands in the middle of some other line. It stayed
# latent for as long as the file only changed between deploys; on 2026-09-07 a
# deploy shipped a change to this script and ran it in the same breath.
#
# Bash parses a function completely before executing any of it, and the final
# `main "$@"; exit $?` is read as one line, so nothing is read from disk after
# main returns. The body is intentionally NOT re-indented — the wrap is a safety
# property, and a whitespace-only diff over 180 lines would bury it.

main() {

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

# ─── Host settings ───────────────────────────────────────────────────────────

# Optional per-host settings (WHG_NO_SUDO, WHG_SITES_DIR, WHG_NGINX_DIR, WHG_COMPOSE ...). Absent on
# the current servers, so they behave exactly as before.
DEPLOY_ENV_FILE="${WHG_DEPLOY_ENV:-$HOME/sites/deploy.env}"
if [ -f "$DEPLOY_ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$DEPLOY_ENV_FILE"
    set +a
fi
SUDO="$(pick_sudo)"
COMPOSE="$(pick_compose) $COMPOSE_ARGS"

# ─── Set environment-specific variables ──────────────────────────────────────

if [ "$ENV" = "prod" ]; then
    if [ -n "$BRANCH_OVERRIDE" ]; then
        echo "Error: --branch is not allowed for production (always uses $PROD_BRANCH)."
        exit 1
    fi
    SITE_DIR="$PROD_DIR"
    BRANCH="$PROD_BRANCH"
    ENV_CONTEXT="$PROD_ENV_CONTEXT"
    PREFIX="${ENV_CONTEXT}_${BRANCH}"
else
    SITE_DIR="$DEV_DIR"
    BRANCH="${BRANCH_OVERRIDE:-$DEV_BRANCH}"
    # Match load_env.py branch normalization used by docker-compose template.
    BRANCH_SAFE="${BRANCH//\//--}"
    ENV_CONTEXT="$DEV_ENV_CONTEXT"
    PREFIX="${ENV_CONTEXT}_${BRANCH_SAFE}"
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
    run_admin_py ./server-admin/set_image_tag.py --site "$ENV_CONTEXT" --tag "$IMAGE_TAG"
    echo ""
fi

echo "── Regenerating config..."
run_admin_py ./server-admin/load_env.py
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
        elif [ -n "$IMAGE_TAG" ]; then
            # `docker compose restart` restarts the containers that already exist and
            # never re-reads `image:`, so a plain restart would leave the stack on the
            # old image while env_template.py claimed the new one — silently, which is
            # the failure set_image_tag.py exists to prevent. `up -d` is what re-reads
            # it.
            #
            # But name the services, and pass --no-deps. A bare `up -d` brings up the
            # WHOLE stack: it recreated postgres on dev on 2026-09-07, which nobody
            # running a flag called --image= expects a database container to be in
            # scope for, and it starts everything at once — which OOM-killed celery
            # twice on a host sitting at 9G/15G with nothing free. Only the services
            # that actually run the WHG image need to move.
            #
            # The list is DERIVED, not hand-maintained: ask compose which services
            # exist, skip the ones with no running container, and keep those whose
            # container actually runs the WHG image. A hardcoded list goes stale when
            # a service is added, and mapping service names to container names by hand
            # gets it wrong — prod's `flower` service is `celery-flower_<prefix>`, not
            # `flower_<prefix>`, so a hand-written mapping silently left prod's flower
            # on the old image. Only RUNNING containers are named, so nothing that was
            # deliberately stopped gets started.
            SVC_LIST="$(whg_image_services "$PREFIX" | tr '\n' ' ')"
            IMAGE_SERVICES=""
            [ -z "${SVC_LIST// /}" ] || IMAGE_SERVICES=" $SVC_LIST"
            if [ -z "$IMAGE_SERVICES" ]; then
                # Nothing recognisable is running; the earlier -z "$RUNNING" branch
                # should have caught this, so say so rather than guessing wider.
                echo "── No running WHG-image containers found for $PREFIX; nothing to move."
                exit 1
            fi
            echo "── Moving onto image $IMAGE_TAG:$IMAGE_SERVICES"
            # shellcheck disable=SC2086  # deliberate word-splitting of the service list
            $COMPOSE up -d --no-deps $IMAGE_SERVICES
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

}

# One line on purpose: bash has both commands in hand before main runs, so it
# never reads from the (possibly rewritten) file again.
main "$@"; exit $?
