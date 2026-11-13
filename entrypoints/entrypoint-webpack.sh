#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Navigate to the working directory
cd /app

npm install

# Prepare static directory - if the web container is also starting up, it will probably have already done this
echo "Preparing static directory..."
if [ -d /app/static ]; then
    echo "/app/static already exists"
else
    echo "/app/static does not exist. Creating directory..."
    mkdir -p /app/static
    chown -R "${USER_NAME}:${USER_NAME}" /app/static
fi

# Start Webpack
echo "Starting Webpack..."
npx webpack --watch --config webpack.config.js
