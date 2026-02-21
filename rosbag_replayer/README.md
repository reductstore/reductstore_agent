# rosbag_replayer

# Rosbag Replayer Node

## Overview
The `Rosbag Replayer` node is a ROS 2 node designed to replay data from rosbag2 files. It reads messages from a specified rosbag file and republishes them on their respective topics, allowing for simulation and testing of ROS-based systems.

## Features
- Supports replaying messages from rosbag2 files.
- Dynamically creates publishers for topics in the bag.
- Allows configuration of the rosbag file path via launch files or parameters.
- Compatible with MCAP storage format.

## Parameters
The node accepts the following parameters:

| Parameter Name   | Type   | Default Value         | Description                                      |
|------------------|--------|-----------------------|--------------------------------------------------|
| `bag_path`       | string | `testdata/demo.mcap`  | Path to the rosbag file to replay.               |
| `playback_rate`  | double | `1.0`                 | Playback speed multiplier (1.0 = real-time).     |
| `log_level`      | string | `info`                | Logging level for the node.                      |

### Playback Rate Examples
- `1.0` = Real-time (original recording speed)
- `0.5` = Half speed (twice as slow)
- `2.0` = Double speed (twice as fast)

## Launch File Usage
A launch file is provided to simplify the usage of the `Rosbag Replayer` node. The launch file allows you to specify the `bag_path` and other parameters.

### Example Launch Command
```bash
ros2 launch reductstore_agent rosbag_replayer_launch.py bag_path:=/path/to/your/rosbag.mcap
```

### Running with ros2 run
```bash
# Real-time playback with custom bag path
ros2 run reductstore_agent rosbag_replayer --ros-args \
  -p bag_path:=/path/to/your/rosbag.mcap \
  -p playback_rate:=1.0

# Half speed playback
ros2 run reductstore_agent rosbag_replayer --ros-args \
  -p bag_path:=/path/to/your/rosbag.mcap \
  -p playback_rate:=0.5
```

### Launch File Arguments
| Argument Name | Default Value         | Description                          |
|---------------|-----------------------|--------------------------------------|
| `bag_path`    | `demo_bags/test.mcap` | Path to the rosbag file to replay.   |
| `log_level`   | `info`               | Logging level for the node.          |

## Node Behavior
1. **Parameter Declaration**:
   - The node declares the `bag_path` parameter with a default value.
   - If launched with a launch file, the value provided in the launch file overrides the default.

2. **Publisher Creation**:
   - The node dynamically creates publishers for all topics in the rosbag file.

3. **Message Replay**:
   - Messages are read from the rosbag file and published on their respective topics.

## Error Handling
- If the `bag_path` parameter is not set or the file does not exist, the node will log an error and terminate.

## Dependencies
- ROS 2 (Humble or later)


