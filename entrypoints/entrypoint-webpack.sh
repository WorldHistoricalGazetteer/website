#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Navigate to the working directory
cd /app

# Only run npm install if node_modules is missing or empty
if [ ! -d /app/node_modules ] || [ -z "$(ls -A /app/node_modules 2>/dev/null)" ]; then
    echo "Installing npm dependencies..."
    npm install
else
    echo "node_modules already exists, skipping npm install."
fi

# Prepare static directory - if the web container is also starting up, it will probably have already done this
echo "Preparing static directory..."
if [ -d /app/static ]; then
    echo "/app/static already exists"
else
    echo "/app/static does not exist. Creating directory..."
    mkdir -p /app/static
    chown -R "${USER_NAME}:${USER_NAME}" /app/static
fi

# Start Webpack with constrained Node.js heap
echo "Starting Webpack..."
export NODE_OPTIONS="--max-old-space-size=1536"
npx webpack --watch --config webpack.config.js
