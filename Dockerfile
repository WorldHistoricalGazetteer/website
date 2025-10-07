FROM python:3.10.7-slim-bullseye

LABEL maintainer="WHC @ Pitt"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MAX_MAP_COUNT=262144

ARG USER_NAME

COPY ./requirements.txt /tmp/requirements.txt
    
RUN set -eux; \
    # Update package lists and install necessary packages \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        # Build tools
        build-essential \
        python3-gdal \
        libpq-dev \
        # System utilities
        curl \
        rsync \
        file \
        gpgv \
        lsb-release \
        sudo \
        nano \
        locate \
        netcat \
        procps \
        psmisc \
        # Version control
        git && \
    # Set up fresh Python virtual environment
    rm -rf /py && \
    python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    /py/bin/pip install -r /tmp/requirements.txt && \
    # Clean up unused packages, lists, and temporary files to reduce image size
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false && \
    rm -rf /var/lib/apt/lists/* /tmp && \
	# Add group and user
	groupadd -g 1000 "$USER_NAME" && \
	useradd -rm -d "/home/$USER_NAME" -g "$USER_NAME" -s "/bin/bash" -G sudo -u 1000 "$USER_NAME" -p "$(openssl passwd -1 change_me)" && \
	# Create a new sudoers file for the user
	echo "$USER_NAME ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/"$USER_NAME" && \
    # Set ownership
    chown -R 1000:0 /py/lib/python3.10/site-packages/guardian/migrations/

WORKDIR /app

ENV PATH="/py/bin:$PATH"

USER "$USER_NAME"

# Each service has a different `entrypoint.sh`, mounted in the compose file
ENTRYPOINT ["/entrypoint.sh"]
