#!/usr/bin/env bash
set -euo pipefail

# ----- pretty logs (auto-disable colors when not a TTY) -----
if [ -t 2 ]; then
  RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
  RED=''; GREEN=''; YELLOW=''; NC=''
fi
log_info()  { printf "%b[INFO]%b %s\n"  "$GREEN" "$NC" "$*" >&2; }
log_warn()  { printf "%b[WARN]%b %s\n"  "$YELLOW" "$NC" "$*" >&2; }
log_error() { printf "%b[ERROR]%b %s\n" "$RED" "$NC" "$*" >&2; }

CONFIG_NAME="params.yml"
DEFAULT_CONFIG="$SNAP/share/reductstore_agent/config/$CONFIG_NAME"
COS_CONFIG="$SNAP_COMMON/configuration/$CONFIG_NAME"
USER_CONFIG="$SNAP_USER_COMMON/$CONFIG_NAME"
SYSTEM_CONFIG="$SNAP_COMMON/$CONFIG_NAME"

# ----- choose config: COS → user → system → (else error with guidance) -----
CONFIG_FILE=""
if   [ -f "$COS_CONFIG" ];    then CONFIG_FILE="$COS_CONFIG"
elif [ -f "$USER_CONFIG" ];   then CONFIG_FILE="$USER_CONFIG"
elif [ -f "$SYSTEM_CONFIG" ]; then CONFIG_FILE="$SYSTEM_CONFIG"
fi

if [ -z "$CONFIG_FILE" ]; then
  log_error "No configuration file found."
  cat >&2 <<EOF

Search order:
  1) $COS_CONFIG
  2) $USER_CONFIG
  3) $SYSTEM_CONFIG

A bundled sample exists at:
  $DEFAULT_CONFIG

Quick setup (user config):
  mkdir -p "$SNAP_USER_COMMON"
  cp "$DEFAULT_CONFIG" "$USER_CONFIG"
  nano "$USER_CONFIG"

Then re-run:
  reductstore-agent.recorder
EOF
  exit 1
fi

# readability check
if [ ! -r "$CONFIG_FILE" ]; then
  log_error "Configuration file is not readable: $CONFIG_FILE"
  exit 1
fi

log_info "Using configuration: $CONFIG_FILE"
exec ros2 run reductstore_agent recorder --ros-args --params-file "$CONFIG_FILE" "$@"
