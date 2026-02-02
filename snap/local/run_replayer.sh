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

# Check if replayer component is installed
SNAP_BASE="${SNAP%/*}"  # /snap/reductstore-agent
REPLAYER_COMPONENT="${SNAP_BASE}/components/${SNAP_REVISION}/replayer"

if [ ! -d "$REPLAYER_COMPONENT" ]; then
  error "rosbag replayer component not installed."
  error "Install with: sudo snap install reductstore-agent+replayer"
  exit 1
fi

# Add component's Python path
export PYTHONPATH="${REPLAYER_COMPONENT}/usr/lib/python3/dist-packages:${PYTHONPATH:-}"

info "Starting rosbag replayer"
exec ros2 run reductstore_agent rosbag_replayer "$@"
