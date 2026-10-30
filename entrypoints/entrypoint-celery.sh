#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Change user password
#/app/entrypoints/user_pwd.sh

# Wait for the database to be ready
source /app/entrypoints/wait_for_db.sh
wait_for_db

echo "Starting Celery worker..."
# place#338: also consume the `recon` queue (background candidate generation, CELERY_TASK_ROUTES).
# Override with CELERY_QUEUES to run a dedicated recon worker.
exec celery -A whg worker --loglevel=debug -Q "${CELERY_QUEUES:-celery,recon}"

