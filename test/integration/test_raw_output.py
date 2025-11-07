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

"""Test raw_output functionality."""


import asyncio
import time
from typing import Generator

import pytest
import rclpy
from rclpy.parameter import Parameter
from reduct import Client
from reduct.error import ReductError
from std_msgs.msg import String

from reductstore_agent.recorder import Recorder

from ..config.test_recorder_params import (
    as_overrides,
    downsampling_params_none,
    output_format_params_raw,
    pipeline_params,
    storage_params,
)


@pytest.fixture
def raw_output_recorder() -> Generator[Recorder, None, None]:
    """Init a raw_output Recorder node."""
    additional_params = [Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0)]

    all_overrides = (
        as_overrides(
            storage_params(),
            pipeline_params(),
            downsampling_params_none(),
            output_format_params_raw(),
        )
        + additional_params
    )

    rec = Recorder(parameter_overrides=all_overrides)
    yield rec
    rec.destroy_node()


def publish_and_spin_simple(
    publisher_node,
    publisher,
    recorder,
    message: String,
    wait_for_subscription: bool = True,
):
    """Publish a single message and spin nodes to process it."""
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

    # Publish the message
    logger.info(f"Publishing message (size: {len(message.data)} bytes)")
    publisher.publish(message)

    # Allow both nodes to process
    rclpy.spin_once(publisher_node, timeout_sec=0.1)
    rclpy.spin_once(recorder, timeout_sec=0.1)

    # Give recorder additional time to process and upload
    rclpy.spin_once(recorder, timeout_sec=1.0)


def generate_large_string(size_kb: int = 150) -> String:
    """Generate a String message larger than the 100 KB batch limit."""
    # 1024 bytes/KB * 150 KB = 153,600 bytes (Guaranteed to trigger streaming)
    data_size_bytes = size_kb * 1024
    msg = String()
    msg.data = "X" * data_size_bytes
    return msg


def test_raw_output_streams_large_record(
    reduct_client, publisher_node, publisher, raw_output_recorder
):
    """Test that Recorder streams large messages immediately."""
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    large_msg = generate_large_string(size_kb=150)

    # Publish the large message and process it
    publish_and_spin_simple(
        publisher_node,
        publisher,
        raw_output_recorder,
        large_msg,
        wait_for_subscription=True,
    )

    async def fetch_and_verify_record(timeout=10.0, retry_interval=0.5):
        """Fetch the record and check the stream's integrity, with retries."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                bucket = await reduct_client.get_bucket(BUCKET_NAME)

                async for record in bucket.query(ENTRY_NAME, when={"$limit": 1}):

                    assert record.labels["serialization"] == "cdr"
                    assert record.labels["type"] == "std_msgs/msg/String"
                    assert record.labels["topic"] == "/test/topic"

                    data_bytes = await record.read_all()
                    assert len(data_bytes) > (100 * 1024), "Record size is too small"

                    print("[TEST] Large record found and verified successfully.")
                    return True

                print(
                    f"[TEST] Record not found yet, retrying... ({time.time() - start_time:.1f}s elapsed)"
                )
                await asyncio.sleep(retry_interval)

            except ReductError as e:
                if e.status_code == 404:
                    print(f"[TEST] Bucket or Entry not found (404), retrying... {e}")
                    await asyncio.sleep(retry_interval)
                else:
                    raise e
            except Exception as e:
                print(f"[TEST] An unexpected error occurred, retrying... {e}")
                await asyncio.sleep(retry_interval)

        print(f"[TEST] Timeout after {timeout}s: Failed to retrieve record.")
        return False

    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(fetch_and_verify_record())
        loop.run_until_complete(task)
        assert task.result() is True, "Failed to retrieve the streamed large record."
    else:
        assert (
            loop.run_until_complete(fetch_and_verify_record()) is True
        ), "Failed to retrieve the streamed large record."
