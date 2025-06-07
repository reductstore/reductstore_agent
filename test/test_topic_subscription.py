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


import pytest
from rclpy.parameter import Parameter
from std_msgs.msg import String

from ros2_reduct_agent.recorder import Recorder


def recorder_factory(include_topics):
    """Create a Recorder instance with specified topics."""
    params = [
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        Parameter(
            "pipelines.test.include_topics",
            Parameter.Type.STRING_ARRAY,
            include_topics,
        ),
        Parameter(
            "pipelines.test.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.test.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
    ]
    recorder = Recorder(parameter_overrides=params)
    return recorder


def test_recorder_subscribed_to_topic(publisher_node):
    """Recorder should subscribe to the configured topic."""
    publisher_node.create_publisher(String, "/unique/test/topic", 10)
    recorder = recorder_factory(["/unique/test/topic"])
    info = recorder.get_subscriptions_info_by_topic("/unique/test/topic")

    assert len(info) == 1, "Expected one subscription"
    assert info[0].topic_type == "std_msgs/msg/String"

    recorder.destroy_node()


def test_recorder_not_subscribed_to_other_topic(publisher_node):
    """Recorder should not subscribe to topics outside the include list."""
    publisher_node.create_publisher(String, "/other/topic", 10)
    recorder = recorder_factory(["/test/topic"])
    info = recorder.get_subscriptions_info_by_topic("/other/topic")

    assert info == [], "Recorder subscribed to /other/topic unexpectedly"

    recorder.destroy_node()


def test_recorder_subscribed_to_multiple_topics(publisher_node):
    """Recorder should subscribe only to the listed topics and ignore others."""
    publisher_node.create_publisher(String, "/test/topic1", 10)
    publisher_node.create_publisher(String, "/test/topic2", 10)

    recorder = recorder_factory(["/test/topic1", "/test/topic2", "/other/topic"])

    info1 = recorder.get_subscriptions_info_by_topic("/test/topic1")
    info2 = recorder.get_subscriptions_info_by_topic("/test/topic2")
    info3 = recorder.get_subscriptions_info_by_topic("/other/topic")

    assert len(info1) == 1 and info1[0].topic_type == "std_msgs/msg/String"
    assert len(info2) == 1 and info2[0].topic_type == "std_msgs/msg/String"
    assert info3 == [], "Recorder subscribed to /other/topic unexpectedly"

    recorder.destroy_node()
