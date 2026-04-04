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

GUNICORN_WORKERS="${GUNICORN_WORKERS:-4}"
echo "Starting Gunicorn server with ${GUNICORN_WORKERS} workers..."
exec gunicorn whg.wsgi:application --bind 0.0.0.0:${APP_PORT} --timeout 1200 -w ${GUNICORN_WORKERS}
