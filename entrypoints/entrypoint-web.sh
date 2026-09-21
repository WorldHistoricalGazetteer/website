#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Ensure required environment variables are set
: "${USER_NAME:?Environment variable USER_NAME is not set}"
: "${APP_PORT:?Environment variable APP_PORT is not set}"

# Prepare static directory
echo "Preparing static directory..."
if [ -d /app/static ]; then
    echo "/app/static already exists"
else
    echo "/app/static does not exist. Creating directory..."
    mkdir -p /app/static
    chown -R "${USER_NAME}:${USER_NAME}" /app/static
fi

# Change user password
#/app/entrypoints/user_pwd.sh

# Collect static files from app directories (datasets/static, main/static, etc.)
# Note: Webpack bundles are already in static/webpack/ (committed to Git),
# so collectstatic only copies files from STATICFILES_DIRS
echo "Collecting static files..."
/py/bin/python manage.py collectstatic --no-input

# Wait for the database to be ready
source /app/entrypoints/wait_for_db.sh
wait_for_db

## Run migrations - disabled because it is safer to do this manually when required
#echo "Running migrations..."
#sudo /py/bin/python manage.py migrate

# place#274 — THREADED workers, so one slow request cannot occupy a whole worker.
#
# Previously 4 SYNCHRONOUS workers. A single POST /reconcile holds its worker for
# the entire fan-out, not for one gateway call: process_queries fans out to
# RECON_FANOUT (8) concurrent gateway calls at CRC_GATEWAY_TIMEOUT (10s) each, so
# a batch of N runs as ceil(N/8) waves and the worker is held for all of them —
# up to ~70s at the advertised maximum of 50. With only 4 workers, a slow gateway
# therefore took the WHOLE SITE down with 503s rather than merely slowing
# reconciliation. --timeout 1200 meant gunicorn never reaped them either.
#
# gthread rather than more sync workers, for a measured reason: the prod host runs
# at ~11 of 15 GB with WordPress, GlitchTip and ollama also resident, and each
# sync worker is a whole Django process. The work being waited on is pure network
# I/O against the gateway, which is exactly what threads fix, at ~no memory cost.
#
# 🛑 This raises in-flight gateway calls from 4x8=32 to 16x8=128, so it AMPLIFIES
# gateway saturation on its own. It ships in the same change as place#268's
# query-rate limiter deliberately: the brake must not arrive after the amplifier.
# Do not raise GUNICORN_THREADS without checking RECON_QUERY_RATE.
GUNICORN_WORKERS="${GUNICORN_WORKERS:-4}"
GUNICORN_THREADS="${GUNICORN_THREADS:-4}"
echo "Starting Gunicorn server with ${GUNICORN_WORKERS} workers x ${GUNICORN_THREADS} threads (gthread)..."
exec gunicorn whg.wsgi:application --bind 0.0.0.0:${APP_PORT} --timeout 1200 \
     -w ${GUNICORN_WORKERS} -k gthread --threads ${GUNICORN_THREADS}
