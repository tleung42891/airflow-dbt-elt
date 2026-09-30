#!/usr/bin/env bash
# setup-docker.sh — verify Docker + Compose v2 are available; optionally install.
#
# Env: INSTALL_DOCKER=true to install Docker Desktop via Homebrew (macOS only).
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if ! command -v docker >/dev/null 2>&1; then
  if [[ "$INSTALL_DOCKER" == true ]]; then
    [[ "$(uname -s)" == "Darwin" ]] || die "INSTALL_DOCKER only supports macOS/Homebrew. Install Docker manually: https://docs.docker.com/get-docker/"
    command -v brew >/dev/null 2>&1 || die "Homebrew not found. Install it first: https://brew.sh"
    log "Installing Docker Desktop via Homebrew..."
    brew install --cask docker
    log "Launching Docker Desktop (first launch may need GUI confirmation)..."
    open -a Docker
  else
    die "Docker not found. Re-run with --install-docker (macOS) or install it: https://docs.docker.com/get-docker/"
  fi
fi

log "Waiting for the Docker daemon..."
for i in $(seq 1 60); do
  docker info >/dev/null 2>&1 && break
  [[ $i -eq 60 ]] && die "Docker daemon did not become ready after 5 minutes."
  sleep 5
done

docker compose version >/dev/null 2>&1 || die "Docker Compose v2 not found (need the 'docker compose' plugin)."
log "Docker $(docker --version | sed 's/Docker version //;s/,.*//') + Compose OK."
