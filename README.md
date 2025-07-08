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
* **Ubuntu Core**: Future Snap integration to support deployment as part of the [Ubuntu Core observability stack](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry).

## Structure

The agent is configured using a YAML file. Each pipeline is an independent logging unit (only one type of pipeline is supported at the moment where all topics are recorded continuously without filtering).

```yaml
/**/*:
  ros__parameters:
    storage: # local ReductStore instance
      url: "http://localhost:8383"
      api_token: "access_token"
      bucket: "ros_data"
      quota_type: "FIFO"
      quota_size: "200GB"
    pipelines:
      telemetry:
        filename_mode: "timestamp"
        include_topics:
          - "/camera/.*"
        exclude_topics:
          - "/camera/ignore"
        static_labels:
          source: telemetry
          robot: alpha
        split:
          max_duration_s: 3600
          max_size_bytes: 10000
```

See the [Configuration](#configuration) section for details on available parameters.

## Installing

Build and run in a ROS 2 workspace:

```bash
# 1. Clone your repo and enter the workspace
mkdir -p ~/ros2_ws/src
cd ~/ros2_ws/src
git clone https://github.com/reductstore/reductstore_agent.git
cd ..

# 2. Install system dependencies
rosdep install --from-paths src --ignore-src -r -y

# 3. Build your package
colcon build --packages-select reductstore_agent

# 4. Source the workspace and run your node
source install/local_setup.bash
ros2 run reductstore_agent recorder --ros-args --params-file ./config.yaml
```

## Configuration

The configuration file is a YAML file that defines the storage settings and pipelines. The `storage` section contains ReductStore connection details, and the `pipelines` section defines the individual pipelines for recording data.

### Storage Configuration

The `storage` section specifies the ReductStore instance to connect to:

 * **`url`**: The URL of the ReductStore instance (e.g., `http://localhost:8383`).
 * **`api_token`**: The API token for authentication. This is required to access the ReductStore instance.
 * **`bucket`**: The bucket name where the data will be stored.
  * **`quota_type`**: The type of quota to apply. Options are:
    * `"FIFO"`: First In, First Out (oldest data is removed first).
    * `"HARD"`: Hard limit (data is not accepted when the quota is reached).
    * `"NONE"`: No quota applied.
  * **`quota_size`**: The size of the quota in bytes. This is required if `quota_type` is set to `"FIFO"` or `"HARD"`.
  * **`max_block_size`**: Maximum size of each block in bytes.
  * **`max_block_records`**: Maximum number of records per block.

More information on how to setup ReductStore can be found in the [ReductStore Getting Started Guide](https://www.reduct.store/docs/getting-started).

### Pipeline Parameters

Each pipeline supports the following parameters:

* **`split`**:

  * **`max_duration_s`**: Maximum duration (in seconds) for each data segment. Must be between `1` and `3600`.
  * **`max_size_bytes`** *(optional)*: Maximum size (in bytes) for each segment. Must be between `1KB` and `1GB`.

* **`chunk_size_bytes`**: Size of each MCAP chunk in bytes. Defaults to `1MB`. Must be between `1KB` and `10MB`.

* **`compression`**: Compression algorithm to use. One of:

  * `"none"`
  * `"lz4"`
  * `"zstd"` *(default)*

* **`enable_crcs`**: Whether to enable CRC checks. Defaults to `true`.

* **`spool_max_size_bytes`**: Maximum in-memory spool size before flushing. Defaults to `10MB`. Must be between `1KB` and `1GB`.

* **`include_topics`**: List of topics to include for recording. Supports regular expressions.
* **`exclude_topics`** *(optional)*: List of topics to exclude from recording. Supports regular expressions.

* **`static_labels`** *(optional)*: Fixed key-value labels to attach to each record.

* **`filename_mode`**: Determines how filenames are generated. One of:

  * `"timestamp"` *(default)* — Use first topic timestamp for filenames.
  * `"incremental"` — Use incrementing numbers (0, 1, 2, ...) for filenames.

## Links

* ReductStore Docs: [https://www.reduct.store/docs/getting-started](https://www.reduct.store/docs/getting-started)
* Ubuntu Core Robotics Telemetry: [https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry)

