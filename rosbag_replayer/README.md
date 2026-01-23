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

| Parameter Name | Type   | Default Value         | Description                          |
|----------------|--------|-----------------------|--------------------------------------|
| `bag_path`     | string | `demo_bags/test.mcap` | Path to the rosbag file to replay.   |
| `log_level`    | string | `info`               | Logging level for the node.          |

## Launch File Usage
A launch file is provided to simplify the usage of the `Rosbag Replayer` node. The launch file allows you to specify the `bag_path` and other parameters.

### Example Launch Command
```bash
ros2 launch reductstore_agent rosbag_replayer_launch.py bag_path:=/path/to/your/rosbag.mcap
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


