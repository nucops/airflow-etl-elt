FROM astrocrpublic.azurecr.io/runtime:3.3-2

LABEL maintainer="Shubham Sharma <shubham@cossth.com>" \
    description="Declarative Airflow container image with pre-baked DuckDB, pandas, and DevContainer toolchains."

# Switch to root to configure system packages and remote development prerequisites
USER root

# Declaratively install system utilities required by Antigravity IDE / VS Code Remote Server
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    git \
    procps \
    ca-certificates \
    jq \
    unzip \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Pre-install Python dependencies at build time for deterministic, zero-delay container startup
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Switch back to non-privileged astro user
USER astro
