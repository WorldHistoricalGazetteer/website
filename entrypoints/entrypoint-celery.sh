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
exec celery -A whg worker --loglevel=debug

