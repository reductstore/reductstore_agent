# Copyright 2025 ReductSoftware UG
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

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message
from rosbag2_py import ConverterOptions, SequentialReader, StorageOptions


class RosbagReplayer(Node):
    """ROS2 Node which replays data from rosbag2 files."""

    def __init__(self) -> None:
        """Initialize the RosbagReplayer node."""
        super().__init__("rosbag_replayer")
        self.bag_path = "demo_bags/test.mcap"
        self._open_reader()
        self.topic_types = {
            topic.name: topic.type for topic in self.reader.get_all_topics_and_types()
        }
        self._topic_publishers = {}
        qos_profile = QoSProfile(depth=10)
        try:
            for topic_name, topic_type in self.topic_types.items():
                pkg, _, msg_type = topic_type.partition("/msg/")
                module = __import__(f"{pkg}.msg", fromlist=[msg_type])
                msg_class = getattr(module, msg_type)
                self.get_logger().info(
                    f"Creating publisher for {topic_name} with type"
                    f" {msg_class} and QoS {type(qos_profile)}"
                )
                publisher = self.create_publisher(msg_class, topic_name, qos_profile)
                self._topic_publishers[topic_name] = publisher
        except Exception as e:
            self.get_logger().error(f"Failed to create publishers: {e}")
            raise

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
            while self.reader.has_next():
                (topic_name, data, timestamp) = self.reader.read_next()
                topic_type = self.topic_types[topic_name]
                pkg, _, msg_type = topic_type.partition("/msg/")
                module = __import__(f"{pkg}.msg", fromlist=[msg_type])
                msg_class = getattr(module, msg_type)
                msg = deserialize_message(data, msg_class)
                # Update timestamp if message has header
                if hasattr(msg, "header") and hasattr(msg.header, "stamp"):
                    msg.header.stamp = self.get_clock().now().to_msg()
                self._topic_publishers[topic_name].publish(msg)
                self.get_logger().debug(
                    f"Published message on topic: {topic_name} at time {timestamp}"
                )
            self.get_logger().info("Reached end of bag, restarting...")
            self._open_reader()


def main():
    """Start the rosbag replayer node."""
    rclpy.init()
    node = RosbagReplayer()
    try:
        node.replay()
    except KeyboardInterrupt:
        pass
    finally:
        if rclpy.ok():
            node.get_logger().info("Destroying node and shutting down ROS...")
            node.destroy_node()
            rclpy.shutdown()


if __name__ == "__main__":
    main()
