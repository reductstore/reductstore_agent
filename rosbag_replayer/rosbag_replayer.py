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

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message
from rosbag2_py import ConverterOptions, SequentialReader, StorageOptions

DEFAULT_BAG_PATH = "testdata/demo.mcap"


class RosbagReplayer(Node):
    """ROS2 Node which replays data from rosbag2 files."""

    def __init__(self, bag_path: str = DEFAULT_BAG_PATH) -> None:
        """Initialize the RosbagReplayer node."""
        super().__init__("rosbag_replayer")

        # Bag path parameter and validation
        if bag_path:
            self.bag_path = bag_path
        else:
            self.bag_path = (
                self.declare_parameter("bag_path", DEFAULT_BAG_PATH)
                .get_parameter_value()
                .string_value
            )

        if not os.path.exists(self.bag_path):
            raise FileNotFoundError(f"Bag path does not exist: {self.bag_path}")

        # Initialize reader and publishers
        self._open_reader()
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
