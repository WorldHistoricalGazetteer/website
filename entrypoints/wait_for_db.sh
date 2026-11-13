#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Function to wait for the database to be ready
wait_for_db() {
  echo "Waiting for the database at $DB_HOST:5432 to be ready..."
  until nc -z "$DB_HOST" 5432; do
    echo "Database not ready yet. Sleeping..."
    sleep 2
  done
  echo "Database is ready."
}
