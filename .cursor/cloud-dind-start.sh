#!/usr/bin/env bash
# Cursor Cloud: start dockerd with the nested-friendly Lab recipe (issue #107).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DAEMON_JSON_SRC="${ROOT}/.cursor/daemon.json"

if ! command -v dockerd >/dev/null 2>&1; then
  echo "cloud-dind-start: dockerd missing; run install (.cursor/cloud-dind-install.sh) first" >&2
  exit 1
fi

sudo mkdir -p /etc/docker
sudo cp "${DAEMON_JSON_SRC}" /etc/docker/daemon.json

if sudo docker info >/dev/null 2>&1; then
  driver="$(sudo docker info --format '{{.Driver}}' 2>/dev/null || true)"
  if [[ "${driver}" != "fuse-overlayfs" ]]; then
    echo "cloud-dind-start: running dockerd uses '${driver}', expected fuse-overlayfs; restarting" >&2
    sudo killall -TERM dockerd containerd 2>/dev/null || true
    sleep 1
    sudo killall -KILL dockerd containerd 2>/dev/null || true
    sleep 1
  else
    # Nested Cloud VMs often leave /var/run as root-only (0700); non-root agents
    # need traverse + docker.sock access without re-login into the docker group.
    sudo chmod 755 /var/run 2>/dev/null || true
    sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
    echo "cloud-dind-start: dockerd already running (fuse-overlayfs)"
    echo "No other long-lived product services required by default; use migraloop lab up for Local Sync Lab."
    exit 0
  fi
fi

sudo dockerd --config-file=/etc/docker/daemon.json >/tmp/cloud-dind-dockerd.log 2>&1 &

ready=0
for _ in $(seq 1 90); do
  if sudo docker info >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "${ready}" -ne 1 ]]; then
  echo "cloud-dind-start: dockerd failed to become ready; see /tmp/cloud-dind-dockerd.log" >&2
  tail -40 /tmp/cloud-dind-dockerd.log >&2 || true
  exit 1
fi

driver="$(sudo docker info --format '{{.Driver}}' 2>/dev/null || true)"
if [[ "${driver}" != "fuse-overlayfs" ]]; then
  echo "cloud-dind-start: expected storage-driver fuse-overlayfs, got '${driver}'" >&2
  exit 1
fi

# Cloud agents run as a non-root user; allow docker CLI without re-login into the docker group.
# Nested Cloud VMs often leave /var/run as root-only (0700); non-root agents need traverse.
sudo chmod 755 /var/run 2>/dev/null || true
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true

echo "cloud-dind-start: dockerd ready (fuse-overlayfs)"
echo "No other long-lived product services required by default; use migraloop lab up for Local Sync Lab."
