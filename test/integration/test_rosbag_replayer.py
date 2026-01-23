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

import rclpy


def test_rosbag_replayer_node_initialization(rosbag_replayer_node):
    """Test that the RosbagReplayer node initializes correctly."""
    # Bag Path
    assert rosbag_replayer_node.bag_path == "testdata/demo.mcap"
    # Topic types and publishers
    assert len(rosbag_replayer_node.topic_types) > 0
    assert len(rosbag_replayer_node._topic_publishers) > 0


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
