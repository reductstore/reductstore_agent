# Copyright 2026 ReductSoftware UG
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""ROS2 Node to produce data for reductstore from rosbag2 files."""

import os
import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message
from rosbag2_py import ConverterOptions, SequentialReader, StorageOptions

DEFAULT_BAG_PATH = "testdata/demo.mcap"


class RosbagReplayer(Node):
    """ROS2 Node which replays data from rosbag2 files."""

    def __init__(self, bag_path: str = None) -> None:
        """Initialize the RosbagReplayer node."""
        super().__init__("rosbag_replayer")

        # Bag path parameter and validation
        # Declare the ROS parameter first
        bag_path_param = self.declare_parameter("bag_path", DEFAULT_BAG_PATH)
        
        # Use provided argument if given, otherwise use ROS parameter
        if bag_path is not None:
            self.bag_path = bag_path
        else:
            self.bag_path = bag_path_param.get_parameter_value().string_value

        if not os.path.exists(self.bag_path):
            raise FileNotFoundError(f"Bag path does not exist: {self.bag_path}")

        # Playback speed control
        playback_rate_param = self.declare_parameter("playback_rate", 1.0)
        self.playback_rate = playback_rate_param.get_parameter_value().double_value
        if self.playback_rate <= 0:
            raise ValueError("playback_rate must be positive")

        # Initialize reader and publishers
        self._open_reader()
        self._reset_playback_state()
        self.topic_types = {
            topic.name: topic.type for topic in self.reader.get_all_topics_and_types()
        }
        self._topic_publishers = {}
        self.qos_profile = QoSProfile(depth=10)
        self._create_publishers()

        # Testing purposes
        self._stop_replaying = False

    def _open_reader(self):
        """Open the rosbag2 reader."""
        self.reader = SequentialReader()
        storage_options = StorageOptions(uri=self.bag_path, storage_id="mcap")
        converter_options = ConverterOptions(
            input_serialization_format="cdr",
            output_serialization_format="cdr",
        )
        self.reader.open(storage_options, converter_options)

    def replay(self) -> None:
        """Continuously replay messages from the rosbag2 file."""
        while rclpy.ok():
            self.get_logger().info("Starting bag replay...")
            while self.reader.has_next() and not self._stop_replaying:
                # Read and deserialize the next message
                topic_name, msg, timestamp = self.read_and_deserialize_message()
                if msg is None:
                    break

                # Throttle playback to respect recorded timing
                self._throttle_playback(timestamp)

                # Update timestamp and publish message
                msg = self.update_message_timestamp(msg)
                self.publish_message(topic_name, msg)
                self.get_logger().debug(
                    f"Published message on topic: {topic_name} at time {timestamp}"
                )

            if not self._stop_replaying:
                # Replayer reached end of bag, restart
                self.get_logger().info("Reached end of bag, restarting...")
                self._open_reader()
                self._reset_playback_state()

    def _reset_playback_state(self) -> None:
        """Reset playback timing state."""
        self._bag_start_time = None
        self._wall_start_time = None

    def _throttle_playback(self, recorded_timestamp_ns: int) -> None:
        """Sleep to match recorded message timing."""
        if self._bag_start_time is None:
            self._bag_start_time = recorded_timestamp_ns
            self._wall_start_time = self.get_clock().now().nanoseconds
            return

        elapsed_bag_ns = recorded_timestamp_ns - self._bag_start_time
        adjusted_elapsed_ns = int(elapsed_bag_ns / self.playback_rate)
        target_wall_ns = self._wall_start_time + adjusted_elapsed_ns
        now_ns = self.get_clock().now().nanoseconds
        wait_ns = target_wall_ns - now_ns

        if wait_ns > 0:
            time.sleep(wait_ns / 1_000_000_000)
        else:
            # Minimum yield to prevent CPU spinning when behind schedule
            time.sleep(0.0001)

    def _create_publishers(self):
        """Create publishers for each topic in the rosbag."""
        try:
            for topic_name, topic_type in self.topic_types.items():
                pkg, _, msg_type = topic_type.partition("/msg/")
                module = __import__(f"{pkg}.msg", fromlist=[msg_type])
                msg_class = getattr(module, msg_type)
                self.get_logger().info(
                    f"Creating publisher for {topic_name} with type"
                    f" {msg_class} and QoS {type(self.qos_profile)}"
                )
                publisher = self.create_publisher(
                    msg_class, topic_name, self.qos_profile
                )
                self._topic_publishers[topic_name] = publisher
        except Exception as e:
            self.get_logger().error(f"Failed to create publishers: {e}")
            raise e

    def read_and_deserialize_message(self):
        """Read and deserialize the next message from the rosbag."""
        if not self.reader.has_next():
            return None, None, None
        topic_name, data, timestamp = self.reader.read_next()
        topic_type = self.topic_types[topic_name]
        pkg, _, msg_type = topic_type.partition("/msg/")
        module = __import__(f"{pkg}.msg", fromlist=[msg_type])
        msg_class = getattr(module, msg_type)
        msg = deserialize_message(data, msg_class)

        return topic_name, msg, timestamp

    def update_message_timestamp(self, msg):
        """Update the timestamp of the message if it has a header."""
        if hasattr(msg, "header") and hasattr(msg.header, "stamp"):
            msg.header.stamp = self.get_clock().now().to_msg()
        return msg

    def publish_message(self, topic_name: str, msg) -> None:
        """Publish a message on the specified topic."""
        if topic_name in self._topic_publishers:
            self._topic_publishers[topic_name].publish(msg)
        else:
            self.get_logger().warning(f"No publisher found for topic: {topic_name}")

    def stop(self):
        """Stop the loop for testing purposes."""
        self._stop_replaying = True


def main():
    """Start the rosbag replayer node."""
    rclpy.init()
    node = None
    try:
        node = RosbagReplayer()
        node.replay()
    except KeyboardInterrupt:
        pass
    except FileNotFoundError as e:
        print(f"Error: {e}")
    finally:
        if node is not None:
            node.get_logger().info("Destroying node and shutting down ROS...")
            node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()