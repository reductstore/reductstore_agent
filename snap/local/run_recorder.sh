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

CONFIG_NAME="params.yml"
DEFAULT_CONFIG="$SNAP/share/reductstore_agent/config/$CONFIG_NAME"
COS_CONFIG="$SNAP_COMMON/configuration/$CONFIG_NAME"
USER_CONFIG="$SNAP_USER_COMMON/$CONFIG_NAME"
SYSTEM_CONFIG="$SNAP_COMMON/$CONFIG_NAME"

# search order: COS, USER, SYSTEM, DEFAULT
CFG=""
if   [ -f "$COS_CONFIG" ];    then CFG="$COS_CONFIG"
elif [ -f "$USER_CONFIG" ];   then CFG="$USER_CONFIG"
elif [ -f "$SYSTEM_CONFIG" ]; then CFG="$SYSTEM_CONFIG"
elif [ -f "$DEFAULT_CONFIG" ]; then CFG="$DEFAULT_CONFIG"
fi

if [ -z "$CFG" ]; then
  error "No configuration found at any of:"
  error "  $COS_CONFIG"
  error "  $USER_CONFIG"
  error "  $SYSTEM_CONFIG"
  error "  $DEFAULT_CONFIG"
  exit 1
fi

[ -r "$CFG" ] || { error "Configuration not readable: $CFG"; exit 1; }

info "Using config: $CFG"
exec ros2 run reductstore_agent recorder --ros-args --params-file "$CFG" "$@"
