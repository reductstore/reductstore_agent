# ros2_reduct_agent

[![Community](https://img.shields.io/discourse/status?server=https%3A%2F%2Fcommunity.reduct.store
)](https://community.reduct.store/signup)

**ros2-reduct-agent** is a ROS 2 node that records selected topics into [ReductStore](https://www.reduct.store/), a high-performance storage and streaming solution. ReductStore is an ELT-based system for robotics and industrial IoT data acquisition. It ingests and streams time-series data of any size—images, sensor readings, logs, files, MCAP, ROS bags—and stores it with time indexing and labels for ultra-fast retrieval and management.

This agent is fully configurable via YAML and designed to solve storage, bandwidth, and workflow limitations commonly found in field robotics. It streams data to ReductStore in near real-time with optional compression, splitting, dynamic labeling, and per-pipeline controls.


- [Container Images](#container-images)
- [ros2_reduct_agent](#ros2_reduct_agent)

## System Requirements

To use this agent, you must have a running instance of ReductStore. You can start a local instance using Docker, install it via Snap or from binaries. Refer to the official guide for setup instructions: [ReductStore Getting Started Guide](https://www.reduct.store/docs/getting-started)

This agent is tested with:
- ROS 2: Jazzy Jalisco
- OS: Ubuntu 24.04 (Noble)
- Python: 3.12

## Motivation

* **Continuous recording**: Prevent oversized rosbag files by splitting recordings by time, size, or topic groups.
* **Bandwidth constraints**: Filter and compress data before optionally replicating to a central server or the cloud.
* **Manual workflows**: Replace manual drive swaps, custom scripts, and bag handling with automated data management.
* **Lack of filtering**: Apply dynamic labels (e.g., mission ID) to tag, search, and retrieve specific data segments.
* **Ubuntu Core**: Future Snap integration to support deployment as part of the [Ubuntu Core observability stack](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry).

## Structure

The agent is configured using a YAML file. Each pipeline is an independent logging unit (only one type of pipeline is supported at the moment where all topics are recorded continuously without filtering).

```yaml
recorder:
  storage: # local ReductStore instance
    url: "http://localhost:8383"   
    api_token: "access_token"
    bucket: "ros-data"
  pipelines:
    telemetry:
      entry: telemetry # entry name in ReductStore
      output_format: mcap # only mcap is supported as of now
      # NOTE: All topics are recorded continuously. Topic filtering will be supported in future versions.
      split:
        max_duration_s: 300
        max_size_bytes: 250_000_000
```

## Installing

Build and run in a ROS 2 workspace:

```bash
mkdir -p ~/ros2_ws/src
cd ~/ros2_ws/src
git clone https://github.com/reductstore/ros2-reduct-agent.git
cd ..
colcon build --packages-select ros2_reduct_agent
source install/local_setup.bash
ros2 run ros2_reduct_agent recorder_node --ros-args --params-file ./config.yaml
```

## Configuration

The configuration file is a YAML file that defines the storage settings and pipelines. The `storage` section contains ReductStore connection details, including the URL, API token, and bucket name. The `pipelines` section defines the individual pipelines for recording data.

Each pipeline has the following parameters:
* `entry`: The name of the entry in ReductStore where the data will be stored.
* `output_format`: The format of the output data. Currently, only `mcap` is supported.
* `split`: A dictionary that specifies how to split the data. It can be based on maximum duration or size. The `max_duration_s` key specifies the maximum duration in seconds for each split, while the `max_size_bytes` key specifies the maximum size in bytes for each split.

### Container Images

| Description | Image:Tag | Default Command |
| --- | --- | -- |
|  |  |  |

### Subscribed Topics

| Topic | Type | Description |
| --- | --- | --- |
|  |  |  |

### Published Topics

| Topic | Type | Description |
| --- | --- | --- |
|  |  |  |

### Services

| Service | Type | Description |
| --- | --- | --- |
|  |  |  |

### Actions

| Action | Type | Description |
| --- | --- | --- |
|  |  |  |

### Parameters

| Parameter | Type | Description |
| --- | --- | --- |
|  |  |  |

## Links

* ReductStore Docs: [https://www.reduct.store/docs/getting-started](https://www.reduct.store/docs/getting-started)
* Ubuntu Core Robotics Telemetry: [https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry)
