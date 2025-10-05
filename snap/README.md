# ReductStore Agent Snap Package

This snap provides the **ReductStore Agent ROS 2 node**, which records ROS 2 topics and streams them into ReductStore.

## Quick Start

### Build and Install

```bash
# Install snapcraft if not already installed
sudo snap install snapcraft --classic

# Build the snap
snapcraft pack

# Install locally (for testing)
sudo snap install --dangerous --devmode reductstore-agent_*.snap
```

### Configure

```bash
# Copy default configuration
mkdir -p ~/snap/reductstore-agent/common
cp /snap/reductstore-agent/current/share/reductstore_agent/config/params.yml \
   ~/snap/reductstore-agent/common/params.yml

# Edit it to fit your setup
nano ~/snap/reductstore-agent/common/params.yml
```

### Run

```bash
# Start the recorder
reductstore-agent.recorder
```

## Configuration Locations

The snap checks configs in this order:

1. `~/snap/reductstore-agent/common/params.yml` (user)
2. `/var/snap/reductstore-agent/common/params.yml` (system)
3. Built-in default: `/snap/reductstore-agent/current/share/reductstore_agent/config/params.yml`

User config is recommended.

## Dependencies

The snap bundles everything needed:

* ROS 2 Jazzy
* Python packages: `reduct-py`, `mcap`, `mcap-ros2-support`
* ROS 2 packages: `rosbag2-py`, `rosbag2-storage-mcap`
