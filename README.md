# ros2-reduct-agent

[![Community](https://img.shields.io/discourse/status?server=https%3A%2F%2Fcommunity.reduct.store
)](https://community.reduct.store/signup)

## Project Description

**ros2-reduct-agent** is a ROS 2 node that records selected topics into [ReductStore](https://www.reduct.store/), a high-performance storage and streaming solution. ReductStore is an ELT-based system for robotics and industrial IoT data acquisition. It ingests and streams time-series data of any size—images, sensor readings, logs, files, MCAP, ROS bags—and stores it with time indexing and labels for ultra-fast retrieval and management.

This agent is fully configurable via YAML and designed to solve storage, bandwidth, and workflow limitations commonly found in field robotics. It streams data to ReductStore in near real-time with optional compression, splitting, dynamic labeling, and per-pipeline controls.

## System Requirements

To use this agent, you must have a running instance of ReductStore. You can start a local instance using Docker, install it via Snap or from binaries. Refer to the official guide for setup instructions: [ReductStore Getting Started Guide](https://www.reduct.store/docs/getting-started)

## Motivation

* **Continuous recording**: Prevent oversized rosbag files by splitting recordings by time, size, or topic groups.
* **Bandwidth constraints**: Filter and compress data before optionally replicating to a central server or the cloud.
* **Manual workflows**: Replace manual drive swaps, custom scripts, and bag handling with automated data management.
* **Lack of filtering**: Apply dynamic labels (e.g., mission ID) to tag, search, and retrieve specific data segments.
* **Ubuntu Core**: Future Snap integration to support deployment as part of the [Ubuntu Core observability stack](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry).

## Structure

The agent is configured using a YAML file. Each pipeline is an independent logging unit:

```yaml
recorder:
  storage: # local ReductStore instance
    url: "http://localhost:8383"   
    api_token: "access_token"
    bucket: "ros-data"
  pipelines:
    telemetry:
      entry: telemetry # entry name in ReductStore
      output_format: mcap # or raw
      compression:
        enabled: true
        type: zstd
      split:
        max_duration_s: 300
        max_size_bytes: 250_000_000
      include_topics:
        - /odom
        - /cmd_vel
        - /imu/data
        - /battery_state
        - /mission_info
      downsample:
        type: none
        value: 1
      labels:
        static: # static labels to all messages
          category: telemetry
        dynamic: # based on live topics
          topic: /mission_info
          fields:
            mission_id: mission_id
            operator: operator
```

Other examples include:


* [`all_topics`](#log-all-topics) for a full dump of all topics
* [`logs`](#logs-pipeline) for `/rosout` and diagnostics
* [`camera_preview`](#camera_preview-pipeline) with downsampling and JPEG topics
* [`full_sensors`](#full_sensors-pipeline) for raw sensor data.

Dynamic labels allow the agent to attach metadata from live ROS 2 topics to each recorded message. For example, you can extract `mission_id` and `operator` fields from a topic like `/mission_info`, and those labels will be applied to every record stored during that mission. This enables label-based filtering and retrieval in ReductStore.

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
* `output_format`: The format of the output data. It can be either `mcap` or `raw`.
* `compression`: A dictionary that specifies whether compression is enabled and the type of compression to use. Supported type is `zstd`.
* `split`: A dictionary that specifies how to split the data. It can be based on maximum duration or size. The `max_duration_s` key specifies the maximum duration in seconds for each split, while the `max_size_bytes` key specifies the maximum size in bytes for each split.
* `include_topics`: A list of topics to include in the pipeline. Only the specified topics will be recorded.
* `include_regex`: A regular expression to include topics based on their names. Only topics matching the regex will be recorded.
* `exclude_regex`: A regular expression to exclude topics based on their names. Topics matching the regex will not be recorded.
* `downsample`: A dictionary that specifies how to downsample the data. It can be based on maximum rate or stride. The `type` key specifies the downsampling method, and the `value` key specifies the downsampling value. Supported types are:
  * `none`: No downsampling.
  * `max_rate`: Downsample to a maximum rate (in Hz).
  * `stride`: Downsample by taking every nth message.
* `labels`: A dictionary that specifies the labels to be applied to the recorded data. It can contain static labels (applied to all messages) and dynamic labels (based on live topics). The `topic` key specifies the topic from which to extract the labels, and the `fields` key specifies the fields to extract.

## Examples

### Log all topics

```yaml
pipelines:
  all_topics:
    entry: all_topics
    output_format: mcap
    compression:
      enabled: true
      type: zstd
    split:
      max_duration_s: 60
    include_regex: ".*"
    downsample:
      type: none
      value: 1
    labels:
      static:
        category: full_dump
```

### Logs pipeline

```yaml
pipelines:
  logs:
    entry: logs
    output_format: mcap
    compression:
      enabled: true
      type: zstd
    split:
      max_duration_s: 300
      max_size_bytes: 250_000_000
    include_topics:
      - /rosout
      - /diagnostics
      - /parameter_events
    downsample:
      type: none
      value: 1
    labels:
      static:
        category: logging
      dynamic:
        topic: /mission_info
        fields:
          mission_id: mission_id
```

### camera\_preview pipeline

```yaml
pipelines:
  camera_preview:
    entry: camera_preview
    output_format: raw
    compression:
      enabled: false
    split:
      max_duration_s: 60
    include_regex: "/camera/.*/preview|/camera/low_res/.*"
    downsample:
      type: max_rate
      value: 5 # 5 Hz
    labels:
      static:
        category: downsampled_sensor
        resolution: low
      dynamic:
        topic: /mission_info
        fields:
          mission_id: mission_id
```

### full\_sensors pipeline

```yaml
pipelines:
  full_sensors:
    entry: full_sensors
    output_format: mcap
    compression:
      enabled: true
      type: zstd
    split:
      max_size_bytes: 500_000_000
    include_regex: "/camera/.*|/scan|/point_cloud"
    exclude_regex: "/camera/.*/preview"
    downsample:
      type: stride
      value: 2
    labels:
      static:
        category: full_resolution
      dynamic:
        topic: /mission_info
        fields:
          mission_id: mission_id
```

## Links

* ReductStore Docs: [https://www.reduct.store/docs/getting-started](https://www.reduct.store/docs/getting-started)
* Ubuntu Core Robotics Telemetry: [https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry)
