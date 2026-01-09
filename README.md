# reductstore_agent

[![ROS 2 Jazzy & Rolling](https://img.shields.io/github/actions/workflow/status/reductstore/reductstore_agent/ci.yml?branch=main&label=ROS%202%20CI%20(Jazzy%20%26%20Rolling))](https://github.com/reductstore/reductstore_agent/actions/workflows/ci.yml)

**reductstore_agent** is a ROS 2 node that records selected topics into [ReductStore](https://www.reduct.store/), a high-performance storage and streaming solution. ReductStore is an ELT-based system for robotics and industrial IoT data acquisition. It ingests and streams time-series data of any size—images, sensor readings, logs, files, MCAP, ROS bags—and stores it with time indexing and labels for ultra-fast retrieval and management.

This agent is fully configurable via YAML and designed to solve storage, bandwidth, and workflow limitations commonly found in field robotics. It streams data to ReductStore in near real-time with optional compression, splitting, dynamic labeling, and per-pipeline controls.

## System Requirements

To use this agent, you must have a running instance of ReductStore. You can start a local instance using Docker, install it via Snap or from binaries. Refer to the official guide for setup instructions: [ReductStore Getting Started Guide](https://www.reduct.store/docs/getting-started)

This agent is tested with:
- ROS 2: Jazzy and Rolling
- OS: Ubuntu 24.04 (Noble)
- Python: 3.12

## Motivation

* **Continuous recording**: Prevent oversized rosbag files by splitting recordings by time, size, or topic groups.
* **Bandwidth constraints**: Filter and compress data before optionally replicating to a central server or the cloud.
* **Manual workflows**: Replace manual drive swaps, custom scripts, and bag handling with automated data management.
* **Lack of filtering**: Apply dynamic labels (e.g., mission ID) to tag, search, and retrieve specific data segments.
* **Ubuntu Core**: Distributed as a Snap and suitable for deployment on Ubuntu Core–based robotic systems.

## Documentation 

📘 **ROS 2 ReductStore Agent Documentation**  
👉 https://www.reduct.store/docs/integrations/ros2-agent

📚 **ReductStore Platform Documentation**  
👉 https://www.reduct.store/docs

The website is the **single source of truth** for:
- YAML configuration options
- Pipeline behavior
- Storage quotas
- Compression & downsampling
- MCAP / CDR formats
- Performance tuning

## Quickstart

### 1. Start ReductStore instance
```bash
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

### 2. Install the ROS2 agent (Snap)
```bash
sudo snap install reductstore-agent --edge
```

## 3. Run the recorder
```bash
ros2 run reductstore_agent recorder --ros-args --params-file ./config/params.yml
```

> **Note**  
> The recorder requires a parameters file to start. The Snap package and source
> distribution both include a default configuration suitable for local testing.
## Installation Options

The agent can be installed in two ways:

### Snap Package (Recommended)

This snap provides the **ReductStore Agent ROS 2 node**, which records ROS 2 topics and streams them into ReductStore.

```bash
# Install snapcraft if not already installed
sudo snap install snapcraft --classic

# Build the snap
snapcraft pack

# Install locally (for testing)
sudo snap install --dangerous reductstore-agent_*.snap
```

Or install directly from the store (edge channel for now):

```bash
sudo snap install reductstore-agent --edge
```

### Build from Source 

```bash
# 1. Create a workspace and clone the repository
mkdir -p ~/ros2_ws/src
cd ~/ros2_ws/src
git clone https://github.com/reductstore/reductstore_agent.git
cd ..

# 2. Install system dependencies
rosdep install --from-paths src --ignore-src -r -y

# 3. Create and activate the virtual environment
python3 -m venv .venv --system-site-packages
source .venv/bin/activate

# 4. Install Python dependencies
pip install -U reduct-py mcap mcap-ros2-support

# 5. Build the package with colcon using the venv Python
python -m colcon build --symlink-install

# 6. Source the workspace and run the node
source install/setup.bash
ros2 run reductstore_agent recorder --ros-args --params-file ./config/params.yml
```

## Links

* ReductStore Docs: [https://www.reduct.store/docs/getting-started](https://www.reduct.store/docs/getting-started)
* Ubuntu Core Robotics Telemetry: [https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry)
