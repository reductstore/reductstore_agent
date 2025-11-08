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

import rclpy
from reduct.error import ReductError
from std_msgs.msg import String


def publish_and_spin_simple(
    publisher_node,
    publisher,
    recorder,
    message: String,
    wait_for_subscription: bool = True,
    n_msg: int = 1,
):
    """Publish a message and spin nodes to process it."""
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
        message.data = f"test_data_{i}"
        publisher.publish(message)
        rclpy.spin_once(publisher_node, timeout_sec=0.1)
        rclpy.spin_once(recorder, timeout_sec=0.1)

    # Allow both nodes to process

    # Give recorder additional time to process and upload
    rclpy.spin_once(recorder, timeout_sec=2.0)


def generate_large_string(size_kb: int = 150) -> String:
    """Generate a String message larger than the 100 KB batch limit."""
    # 1024 bytes/KB * 150 KB = 153,600 bytes (Guaranteed to trigger streaming)
    data_size_bytes = size_kb * 1024
    msg = String()
    msg.data = "X" * data_size_bytes
    return msg


async def fetch_and_count_records(
    client,
    bucket_name,
    entry_name,
    expected_count,
    timeout=10.0,
    retry_interval=1.0,
):
    """Fetch record and count it."""
    start_time = time.time()
    last_count = 0

    while time.time() - start_time < timeout:
        record_count = 0
        try:
            bucket = await client.get_bucket(bucket_name)

            # Query all records in the entry
            async for record in bucket.query(entry_name):
                # Basic verification for a batched record
                assert record.labels["serialization"] == "cdr"
                assert record.labels["type"] == "std_msgs/msg/String"
                assert record.labels["topic"] == "/test/topic"
                record_count += 1

            last_count = record_count
            if record_count >= expected_count:
                print(
                    f"[TEST] Success: Found {record_count} records "
                    f"(expected at least {expected_count})."
                )
                return True

            print(
                f"[TEST] Found {record_count} records, retrying for "
                f"{expected_count}... ({time.time() - start_time:.1f}s elapsed)"
            )
            await asyncio.sleep(retry_interval)

        except ReductError as e:
            if e.status_code == 404:
                print(f"[TEST] Entry not found (404), retrying... {e}")
                await asyncio.sleep(retry_interval)
            else:
                raise e
        except Exception as e:
            print(f"[TEST] An unexpected error occurred, retrying... {e}")
            await asyncio.sleep(retry_interval)

    print(
        f"[TEST] Timeout after {timeout}s: Failed to retrieve {expected_count} "
        f"records. Found {last_count}."
    )
    return False


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

    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(
            fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
        )
        loop.run_until_complete(task)
        assert task.result() is True, "Failed to retrieve the streamed large record."
    else:
        assert (
            loop.run_until_complete(
                fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
            )
            is True
        ), "Failed to retrieve the streamed large record."


def test_raw_output_batch_flushes(
    reduct_client, publisher_node, publisher, raw_output_recorder
):
    """Test that the Recorder flushes batch."""
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    msg = generate_large_string(size_kb=90)
    MESSAGE_COUNT = 114
    publish_and_spin_simple(
        publisher_node,
        publisher,
        raw_output_recorder,
        msg,
        wait_for_subscription=True,
        n_msg=MESSAGE_COUNT,
    )

    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(
            fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
        )
        loop.run_until_complete(task)
        assert task.result() is True, "Failed to retrive message."
    else:
        assert (
            loop.run_until_complete(
                fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
            )
            is True
        ), "Failed to retrive message."


def test_raw_output_batch_flushed_on_shutdown(
    reduct_client, publisher_node, publisher, raw_output_recorder
):
    """Test that the Recorder flushes batch on shutdown."""
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    msg = generate_large_string(size_kb=90)

    publish_and_spin_simple(
        publisher_node, publisher, raw_output_recorder, msg, wait_for_subscription=True
    )

    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(
            fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
        )
        loop.run_until_complete(task)
        assert task.result() is True, "Failed to retrive batched message."
    else:
        assert (
            loop.run_until_complete(
                fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME, 1)
            )
            is True
        ), "Failed to retrive batched message."
