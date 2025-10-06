#!/usr/bin/env bash
set -euo pipefail

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

cat <<EOF
ReductStore Agent Configuration Help
====================================

Configuration search order:
  1) COS config       $COS_CONFIG
  2) User config      $USER_CONFIG
  3) System config    $SYSTEM_CONFIG
  4) Bundled default  $DEFAULT_CONFIG

Quick setup (user config):
  mkdir -p "$SNAP_USER_COMMON"
  cp "$DEFAULT_CONFIG" "$USER_CONFIG"
  nano "$USER_CONFIG"

Run the recorder:
  reductstore-agent.recorder

Project info:
  https://github.com/reductstore/reductstore_agent

Current environment:
  SNAP             = ${SNAP:-not set}
  SNAP_USER_COMMON = ${SNAP_USER_COMMON:-not set}
  SNAP_COMMON      = ${SNAP_COMMON:-not set}

Config file status:
EOF

if [ -f "$COS_CONFIG" ]; then
  info "COS config found: $COS_CONFIG"
else
  warn "COS config not found: $COS_CONFIG"
fi

if [ -f "$USER_CONFIG" ]; then
  info "User config found: $USER_CONFIG"
else
  warn "User config not found: $USER_CONFIG"
fi

if [ -f "$SYSTEM_CONFIG" ]; then
  info "System config found: $SYSTEM_CONFIG"
else
  warn "System config not found: $SYSTEM_CONFIG"
fi

if [ -f "$DEFAULT_CONFIG" ]; then
  info "Default bundled config available: $DEFAULT_CONFIG"
else
  error "Default bundled config missing: $DEFAULT_CONFIG"
fi
