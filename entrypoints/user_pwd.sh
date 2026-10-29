#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Print current user and group memberships
echo "Current user: $(whoami)"
echo "Groups: $(groups)"

# Ensure environment variables are set
: "${USER_NAME:?Environment variable USER_NAME is not set}"
: "${WHGADMIN_PASSWORD:?Environment variable WHGADMIN_PASSWORD is not set}"

# Set the user's password using the hashed password
sudo usermod "$USER_NAME" --password "$(openssl passwd -1 "$WHGADMIN_PASSWORD")"
echo "Password changed for $USER_NAME"

# Remove the user's sudoers file
SUDOERS_FILE="/etc/sudoers.d/$USER_NAME"
if [ -f "$SUDOERS_FILE" ]; then
    echo "Removing sudoers file: $SUDOERS_FILE"
    sudo rm "$SUDOERS_FILE"
    echo "Sudoers file removed"
else
    echo "No sudoers file found for $USER_NAME"
fi
