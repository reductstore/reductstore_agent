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

"""Integration test for rosbag_replayer."""

from unittest.mock import patch

import pytest
import rclpy

from rosbag_replayer.rosbag_replayer import RosbagReplayer


def test_rosbag_replayer_node_initialization(rosbag_replayer_node):
    """Test that the RosbagReplayer node initializes correctly."""
    # Bag Path
    assert rosbag_replayer_node.bag_path == "testdata/demo.mcap"
    # Topic types and publishers
    assert len(rosbag_replayer_node.topic_types) > 0
    assert len(rosbag_replayer_node._topic_publishers) > 0


def test_rosbag_replayer_raises_error_for_nonexistent_bag():
    """Test that RosbagReplayer raises FileNotFoundError for non-existent bag path."""
    with pytest.raises(FileNotFoundError, match="Bag path does not exist"):
        RosbagReplayer(bag_path="nonexistent/path/to/bag.mcap")


def test_throttle_playback_sleeps(rosbag_replayer_node):
    """Test that _throttle_playback actually sleeps to prevent CPU spinning."""
    sleep_called = {"count": 0, "total_sleep": 0.0}

    def mock_sleep(duration):
        sleep_called["count"] += 1
        sleep_called["total_sleep"] += duration

    # Initialize playback state with first message
    rosbag_replayer_node._throttle_playback(0)

    # Simulate second message 100ms later in recorded time
    with patch("time.sleep", mock_sleep):
        rosbag_replayer_node._throttle_playback(100_000_000)  # 100ms in nanoseconds

    assert (
        sleep_called["count"] > 0
    ), "Throttle should call time.sleep to prevent CPU spinning"
    assert (
        sleep_called["total_sleep"] > 0
    ), "Throttle should sleep for a positive duration"


def test_throttle_playback_minimum_sleep_when_behind(rosbag_replayer_node):
    """Test that _throttle_playback yields CPU even when behind schedule."""
    sleep_called = {"count": 0, "durations": []}

    def mock_sleep(duration):
        sleep_called["count"] += 1
        sleep_called["durations"].append(duration)

    # Initialize playback state
    rosbag_replayer_node._throttle_playback(0)

    # Simulate being "behind schedule" by setting wall start time far in the past
    # This means target_wall_ns < now_ns, so wait_ns will be negative
    rosbag_replayer_node._wall_start_time = (
        rosbag_replayer_node.get_clock().now().nanoseconds - 1_000_000_000  # 1s in past
    )

    with patch("time.sleep", mock_sleep):
        rosbag_replayer_node._throttle_playback(100_000_000)  # 100ms recorded time

    assert sleep_called["count"] > 0, "Throttle should still sleep when behind schedule"
    assert (
        0.0001 in sleep_called["durations"]
    ), "Should use minimum sleep (0.1ms) when behind"


def test_message_replay(rosbag_replayer_node):
    """Test that messages are replayed correctly from the rosbag."""
    # Define a counter to track iterations
    iteration_counter = {"count": 0}
    max_iterations = 5

    # Patch the `rclpy.ok` method to stop after `max_iterations`
    original_ok = rclpy.ok

    def mock_rclpy_ok():
        if iteration_counter["count"] >= max_iterations:
            return False  # Stop the loop
        iteration_counter["count"] += 1
        return original_ok()

    with patch("rclpy.ok", mock_rclpy_ok):
        rosbag_replayer_node.replay()

    # Check the iteration count
    assert (
        iteration_counter["count"] == max_iterations
    ), f"Expected {max_iterations} iterations, got {iteration_counter['count']}"
