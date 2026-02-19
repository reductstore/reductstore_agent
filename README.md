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

## Quickstart and Installation 

### 1. Start a ReductStore Instance

The agent requires a running ReductStore instance.

For local testing, you can start ReductStore using Docker:

```bash
docker run -p 8383:8383 reductstore/reductstore:latest
```
### 2. Install the ROS 2 Agent

Choose one of the following installation methods:

#### a) Snap Package (Recommended)

```bash
sudo snap install reductstore-agent --edge
```
Optionally you can also enable a [rosbag_replayer node](https://github.com/reductstore/reductstore_agent/tree/main/rosbag_replayer)
```bash
sudo snap set reductstore-agent replayer.enabled=true
```

#### b) Build from Source

```bash
# Create workspace and clone repository
mkdir -p ~/ros2_ws/src
cd ~/ros2_ws/src
git clone https://github.com/reductstore/reductstore_agent.git
cd ..

# Install dependencies
rosdep install --from-paths src --ignore-src -r -y

# Set up virtual environment
python3 -m venv .venv --system-site-packages
source .venv/bin/activate

# Install Python dependencies
pip install -U reduct-py mcap mcap-ros2-support

# Build the package
python -m colcon build --symlink-install
```

### 3. Run the Recorder

```bash
# For both Snap and source installs:
ros2 run reductstore_agent recorder --ros-args --params-file ./config/params.yml

# rosbag_replayer with custom bag path and playback rate
ros2 run reductstore_agent rosbag_replayer --ros-args \
  -p bag_path:=/path/to/your/rosbag.mcap \
  -p playback_rate:=1.0
```

> **Note:** The recorder requires a parameters file to start. Both the Snap and source distributions include a default configuration suitable for local testing.

## Links

* ReductStore Docs: [https://www.reduct.store/docs/getting-started](https://www.reduct.store/docs/getting-started)
* Ubuntu Core Robotics Telemetry: [https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry)
