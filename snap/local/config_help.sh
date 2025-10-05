#!/usr/bin/env bash
set -euo pipefail

cat << 'EOF'
ReductStore Agent Configuration Help
====================================

Configuration file locations (checked in this order at runtime):
1) COS configuration (if connected):
   $SNAP_COMMON/configuration/params.yml
2) User configuration (recommended):
   $SNAP_USER_COMMON/params.yml
3) System configuration:
   $SNAP_COMMON/params.yml
4) Bundled sample (read-only):
   $SNAP/share/reductstore_agent/config/params.yml

Quick setup (user config):
  mkdir -p "$SNAP_USER_COMMON"
  cp "$SNAP/share/reductstore_agent/config/params.yml" "$SNAP_USER_COMMON/params.yml"
  nano "$SNAP_USER_COMMON/params.yml"

Run the recorder:
  reductstore-agent.recorder

More info:
  https://github.com/reductstore/reductstore_agent

Current paths:
EOF

echo "SNAP: ${SNAP:-not set}"
echo "SNAP_USER_COMMON: ${SNAP_USER_COMMON:-not set}"
echo "SNAP_COMMON: ${SNAP_COMMON:-not set}"

# Checks
DEFAULT_CONFIG="$SNAP/share/reductstore_agent/config/params.yml"
COS_CONFIG="$SNAP_COMMON/configuration/params.yml"
USER_CONFIG="$SNAP_USER_COMMON/params.yml"
SYSTEM_CONFIG="$SNAP_COMMON/params.yml"

if [ -f "$DEFAULT_CONFIG" ]; then
  echo "✓ Default config found: $DEFAULT_CONFIG"
else
  echo "✗ Default config missing: $DEFAULT_CONFIG"
fi

if [ -f "$COS_CONFIG" ]; then
  echo "✓ COS config found: $COS_CONFIG"
else
  echo "○ COS config not found: $COS_CONFIG"
fi

if [ -f "$USER_CONFIG" ]; then
  echo "✓ User config found: $USER_CONFIG"
else
  echo "○ User config not found: $USER_CONFIG (you can create it)"
fi

if [ -f "$SYSTEM_CONFIG" ]; then
  echo "✓ System config found: $SYSTEM_CONFIG"
else
  echo "○ System config not found: $SYSTEM_CONFIG"
fi
