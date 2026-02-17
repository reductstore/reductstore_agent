#!/usr/bin/env bash
set -euo pipefail

# colored logs to stderr
if [ -t 2 ]; then
  RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
  RED=''; GREEN=''; YELLOW=''; NC=''
fi
info()  { printf "%bINFO%b  %s\n"  "$GREEN" "$NC" "$*" >&2; }
warn()  { printf "%bWARN%b  %s\n"  "$YELLOW" "$NC" "$*" >&2; }
error() { printf "%bERROR%b %s\n" "$RED" "$NC" "$*" >&2; }

REPLAYER_ENABLED="$(snapctl get replayer.enabled 2>/dev/null || true)"
if [ "$REPLAYER_ENABLED" != "true" ]; then
  error "Replayer is disabled. Enable with: sudo snap set reductstore-agent replayer.enabled=true"
  exit 1
fi

info "Starting rosbag replayer"
exec ros2 run reductstore_agent rosbag_replayer "$@"
