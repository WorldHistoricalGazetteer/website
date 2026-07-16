# syntax=docker/dockerfile:1
FROM python:3.10.7-slim-bullseye

LABEL maintainer="WHC @ Pitt"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MAX_MAP_COUNT=262144 \
    PATH="/py/bin:$PATH"

ARG USER_NAME

# ---------------------------------------------------------------------------
# Layer 1 — system packages. Independent of requirements.txt, so this layer
# stays cached across dependency bumps; it only rebuilds when this list
# changes. (Previously apt + pip shared one RUN, so any requirements.txt edit
# forced the slow apt-get install to re-run too.)
# ---------------------------------------------------------------------------
RUN set -eux; \
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
    # Clean up unused auto-installed packages and apt lists to reduce size
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false && \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Layer 2 — fresh virtualenv (cached unless the base image changes).
# ---------------------------------------------------------------------------
RUN rm -rf /py && \
    python -m venv /py && \
    /py/bin/pip install --upgrade pip

# ---------------------------------------------------------------------------
# Layer 3 — Python dependencies. The COPY makes this layer's cache key the
# CONTENT of requirements.txt, so a dependency bump rebuilds ONLY this layer
# (Layers 1-2 are reused). The BuildKit pip cache mount reuses downloaded
# wheels across builds for a further speed-up.
# ---------------------------------------------------------------------------
COPY ./requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    /py/bin/pip install -r /tmp/requirements.txt

# ---------------------------------------------------------------------------
# Layer 4 — application user + permissions (cheap, rarely changes).
# ---------------------------------------------------------------------------
RUN set -eux; \
    # Add group and user
    groupadd -g 1000 "$USER_NAME" && \
    useradd -rm -d "/home/$USER_NAME" -g "$USER_NAME" -s "/bin/bash" -G sudo -u 1000 "$USER_NAME" -p "$(openssl passwd -1 change_me)" && \
    # Create a new sudoers file for the user
    echo "$USER_NAME ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/"$USER_NAME" && \
    # Set ownership
    chown -R 1000:0 /py/lib/python3.10/site-packages/guardian/migrations/ && \
    # Remove the temporary requirements file (matches previous `rm -rf /tmp`)
    rm -rf /tmp/*

WORKDIR /app

USER "$USER_NAME"

# Each service has a different `entrypoint.sh`, mounted in the compose file
ENTRYPOINT ["/entrypoint.sh"]
