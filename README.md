# ros2-reduct-agent

[![Community](https://img.shields.io/discourse/status?server=https%3A%2F%2Fcommunity.reduct.store
)](https://community.reduct.store/signup)

## Project Description

**ros2-reduct-agent** is a ROS 2 node that records selected topics into [ReductStore](https://www.reduct.store/), a high-performance storage and streaming solution. ReductStore is an ELT-based system for robotics and industrial IoT data acquisition. It captures raw data, ingests and streams data of any size—images, sensor readings, logs, files, ROS bags—and stores it with time indexing and labels for ultra-fast retrieval and management.

This agent is fully configurable via YAML and designed to solve storage, bandwidth, and workflow limitations commonly found in field robotics. It streams data to ReductStore in near real-time with optional compression, splitting, dynamic labeling, and per-pipeline controls.

## System Requirements

To use this agent, you must have a running instance of ReductStore. You can start a local instance using Docker or install it via Snap or from binaries. Refer to the official guide for setup instructions: [ReductStore Getting Started Guide](https://www.reduct.store/docs/getting-started)

## Motivation

* **Limited onboard storage**: Avoid large rosbag files by streaming directly into a FIFO-managed object store.
* **Bandwidth constraints**: Compress and filter data before optional replication to the cloud.
* **Manual workflows**: Eliminate hard-drive swaps, manual bag handling, and custom scripts.
* **Lack of filtering**: Use dynamic labels (e.g., mission ID) to tag and retrieve specific data.
* **Integration gaps**: Designed to work seamlessly on ROS 2 and integrate with ReductStore for long-term access and analysis.
* **Ubuntu Core**: Future Snap integration aligns with [Ubuntu Core’s vision for secure, OTA-updated robotics](https://ubuntu.com/blog/ubuntu-core-24-robotics-telemetry).

## Structure

The agent is configured using a YAML file. Each pipeline is an independent logging unit:

```yaml
recorder:
  storage:
    backend: reductstore
    endpoint: "http://localhost:8383"
  global:
    resource_limits:
      max_cpu_percent: 50
      max_mem_mb: 512
  pipelines:
    telemetry:
      target:
        bucket: telemetry
      output_format: mcap
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
        static:
          category: telemetry
        dynamic:
          topic: /mission_info
          fields:
            mission_id: mission_id
            operator: operator
```

Other examples include:


* [`all_topics`](#log-all-topics) for a full dump of all topics
* [`logs`](#logs-pipeline) for `/rosout` and diagnostics
* [`camera_preview`](#camera_preview-pipeline) with downsampling and JPEG topics
* [`full_sensors`](#full_sensors-pipeline) for raw sensor data with stride

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

## Regular Expressions

Pipelines support topic selection using regular expressions:

* `include_regex`: Record all topics that match this pattern.
* `exclude_regex`: Skip topics matching this pattern (applies after `include_regex`).

Useful when capturing dynamic topic names (e.g. multiple cameras or sensors with dynamic namespaces).

## Examples

### Log all topics

```yaml
pipelines:
  all_topics:
    target:
      bucket: all_topics
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
    target:
      bucket: logs
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
    target:
      bucket: camera_preview
    output_format: raw
    compression:
      enabled: false
    split:
      max_duration_s: 60
    include_regex: "/camera/.*/preview|/camera/low_res/.*"
    downsample:
      type: max_rate
      value: 5
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
    target:
      bucket: full_sensors
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
