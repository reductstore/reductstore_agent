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

"""Utilities for testing purposes."""
import rclpy
from rclpy.publisher import Publisher
from std_msgs.msg import Float32, Int32, String

from reductstore_agent.models import PipelineConfig


async def fetch_and_count_records(
    client,
    bucket_name: str,
    entry_name: str,
):
    """Fetch and return records."""
    bucket = await client.get_bucket(bucket_name)
    output = []
    async for record in bucket.query(entry_name):
        output.append(record)
    return output


def publish_and_spin_messages(
    publisher_node,
    publisher,
    recorder,
    message: String,
    wait_for_subscription: bool = True,
    n_msg: int = 1,
    frequency: float = None,
):
    """Publish messages and spin nodes to process it."""
    logger = publisher_node.get_logger()

    if wait_for_subscription:
        # Wait for recorder to be ready and subscribed
        logger.info("Waiting for recorder to initialize and subscribe...")
        for _ in range(15):  # Up to 3 seconds
            rclpy.spin_once(recorder, timeout_sec=0.2)
            # Check if subscriptions are active by looking at subscriber count
            if len(recorder.subscribers) > 0:
                logger.info(
                    "Recorder subscription detected, proceeding with publish..."
                )
                break
        else:
            logger.warning("Recorder subscription not detected, proceeding anyway...")

    # Publish the messages
    for i in range(n_msg):
        logger.info(f"Publishing message (size: {len(message.data)} bytes)")
        publisher.publish(message)
        # Allow both nodes to process
        rclpy.spin_once(publisher_node, timeout_sec=0.1)
        rclpy.spin_once(recorder, timeout_sec=0.1)
        rclpy.spin_once(recorder, timeout_sec=0.1)
    # Give recorder additional time to process and upload
    rclpy.spin_once(recorder, timeout_sec=2.0)


def generate_string(size_kb: int = 150) -> String:
    """Generate a String message larger than the 100 KB batch limit."""
    # 1024 bytes/KB * 150 KB = 153,600 bytes (Guaranteed to trigger streaming)
    data_size_bytes = size_kb * 1024
    msg = String()
    msg.data = "X" * data_size_bytes
    return msg


def publish_and_spin_messages_multi(
    publisher_node,
    publishers: dict[str, Publisher],
    recorder,
    wait_for_subscription: bool = True,
    n_msg: int = 1,
):
    """Publish messages for dynamic labels and spin nodes to process it."""
    logger = publisher_node.get_logger()

    if wait_for_subscription:
        # Wait for recorder to be ready and subscribed
        logger.info("Waiting for recorder to initialize and subscribe...")
        for _ in range(15):  # Up to 3 seconds
            rclpy.spin_once(recorder, timeout_sec=0.2)
            # Check if subscriptions are active by looking at subscriber count
            if len(recorder.subscribers) >= len(publishers):
                logger.info(
                    "Recorder subscription detected, proceeding with publish..."
                )
                break
        else:
            logger.warning("Recorder subscription not detected, proceeding anyway...")

    # Publish the messages
    for i in range(n_msg):
        current_speed = 50 + i * 10
        current_mission_id = f"Run_{i}"
        current_voltage = 12.0 - i * 0.1

        for topic_name, pub_object in publishers.items():
            if topic_name == "/telemetry":
                msg = Int32()
                msg.data = current_speed

            elif topic_name == "/startup_config":
                msg = Float32()
                msg.data = float(current_voltage)

            elif topic_name == "/mission_info":
                msg = String()
                msg.data = current_mission_id

            else:
                msg = String()
                msg.data = f"generic_msg_{i}"

            pub_object.publish(msg)
            logger.info(f"Publishing cycle {i} on topic: {topic_name}")
            # Allow processing time
            rclpy.spin_once(publisher_node, timeout_sec=0.1)
            rclpy.spin_once(recorder, timeout_sec=0.1)
            rclpy.spin_once(recorder, timeout_sec=0.1)

    rclpy.spin_once(recorder, timeout_sec=2.0)


def make_pipeline_config(name: str) -> PipelineConfig:
    """Create a PipelineConfig with specified parameters for testing."""
    return PipelineConfig(
        **{
            "split.max_duration_s": 1,
            "include_topics": [f"/{name}"],
            "filename_mode": "timestamp",
        }
    )
