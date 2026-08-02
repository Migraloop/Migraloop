#!/usr/bin/env bash
# Cursor Cloud: install nested-friendly Docker for Local Sync Lab.
# Proven recipe (issue #107): fuse-overlayfs + containerd snapshotter disabled.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DAEMON_JSON_SRC="${ROOT}/.cursor/daemon.json"

export DEBIAN_FRONTEND=noninteractive

if ! command -v sudo >/dev/null 2>&1; then
  echo "cloud-dind-install: sudo is required" >&2
  exit 1
fi

sudo apt-get update -qq
sudo apt-get install -y -qq \
  docker.io \
  docker-compose-v2 \
  fuse-overlayfs \
  iptables \
  uidmap

sudo mkdir -p /etc/docker
sudo cp "${DAEMON_JSON_SRC}" /etc/docker/daemon.json
sudo usermod -aG docker "${USER:-ubuntu}" 2>/dev/null || true

# Stop any leftover dockerd from a previous install attempt (no systemd in Cloud VMs).
sudo killall -TERM dockerd containerd 2>/dev/null || true
sleep 1
sudo killall -KILL dockerd containerd 2>/dev/null || true
sleep 1

# Start dockerd long enough to pre-warm Lab images into the environment snapshot.
sudo dockerd --config-file=/etc/docker/daemon.json >/tmp/cloud-dind-install-dockerd.log 2>&1 &
DOCKERD_PID=$!

ready=0
for _ in $(seq 1 90); do
  if sudo docker info >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "${DOCKERD_PID}" 2>/dev/null; then
    echo "cloud-dind-install: dockerd exited during install; see /tmp/cloud-dind-install-dockerd.log" >&2
    tail -40 /tmp/cloud-dind-install-dockerd.log >&2 || true
    exit 1
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "cloud-dind-install: dockerd did not become ready; see /tmp/cloud-dind-install-dockerd.log" >&2
  tail -40 /tmp/cloud-dind-install-dockerd.log >&2 || true
  sudo kill "${DOCKERD_PID}" 2>/dev/null || true
  exit 1
fi

driver="$(sudo docker info --format '{{.Driver}}' 2>/dev/null || true)"
if [[ "${driver}" != "fuse-overlayfs" ]]; then
  echo "cloud-dind-install: expected storage-driver fuse-overlayfs, got '${driver}'" >&2
  sudo kill "${DOCKERD_PID}" 2>/dev/null || true
  exit 1
fi

# Pre-warm Lab Fixture images only after a working nested-friendly driver is confirmed.
sudo docker pull postgres:16
sudo docker pull mongo:7
sudo docker pull gvenzl/oracle-free:23-slim

# Leave dockerd stopped; start script brings it up for the agent session.
sudo killall -TERM dockerd containerd 2>/dev/null || true
sleep 1
sudo killall -KILL dockerd containerd 2>/dev/null || true

echo "cloud-dind-install: Docker ${driver} ready; Lab images pre-warmed"
