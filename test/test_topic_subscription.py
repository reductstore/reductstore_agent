import rclpy
from rclpy.parameter import Parameter
from std_msgs.msg import String

from ros2_reduct_agent.recorder import Recorder


def test_recorder_subscribed_to_topic():
    """Test that the Recorder node is subscribed to the correct topic."""
    publisher = rclpy.create_node("test_publisher")
    publisher.create_publisher(String, "/test/topic", 10)

    recorder = Recorder(
        parameter_overrides=[
            Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
            Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
            Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
            Parameter(
                "pipelines.test.include_topics",
                Parameter.Type.STRING_ARRAY,
                ["/test/topic"],
            ),
            Parameter(
                "pipelines.test.split.max_duration_s", Parameter.Type.INTEGER, 10
            ),
            Parameter("pipelines.test.filename_mode", Parameter.Type.STRING, "counter"),
        ]
    )

    info = recorder.get_subscriptions_info_by_topic("/test/topic")
    assert (
        info[0].topic_type == "std_msgs/msg/String"
    ), "Recorder is not subscribed to /test/topic"
    assert len(info) == 1, "Recorder is subscribed to multiple topics"

    publisher.destroy_node()
    recorder.destroy_node()


def test_recorder_not_subscribed_to_other_topic():
    """Test that the Recorder node is not subscribed to other topics."""
    publisher = rclpy.create_node("test_publisher")
    publisher.create_publisher(String, "/other/topic", 10)

    recorder = Recorder(
        parameter_overrides=[
            Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
            Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
            Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
            Parameter(
                "pipelines.test.include_topics",
                Parameter.Type.STRING_ARRAY,
                ["/test/topic"],
            ),
            Parameter(
                "pipelines.test.split.max_duration_s", Parameter.Type.INTEGER, 10
            ),
            Parameter("pipelines.test.filename_mode", Parameter.Type.STRING, "counter"),
        ]
    )

    info = recorder.get_subscriptions_info_by_topic("/other/topic")
    assert (
        len(info) == 0
    ), "Recorder is subscribed to /other/topic when it should not be"

    publisher.destroy_node()
    recorder.destroy_node()


def test_recorder_subscribed_to_multiple_topics():
    """Test that the Recorder node is subscribed to multiple topics."""
    publisher = rclpy.create_node("test_publisher")
    publisher.create_publisher(String, "/test/topic1", 10)
    publisher.create_publisher(String, "/test/topic2", 10)

    recorder = Recorder(
        parameter_overrides=[
            Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
            Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
            Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
            Parameter(
                "pipelines.test.include_topics",
                Parameter.Type.STRING_ARRAY,
                ["/test/topic1", "/test/topic2", "/other/topic"],
            ),
            Parameter(
                "pipelines.test.split.max_duration_s", Parameter.Type.INTEGER, 10
            ),
            Parameter("pipelines.test.filename_mode", Parameter.Type.STRING, "counter"),
        ]
    )

    info1 = recorder.get_subscriptions_info_by_topic("/test/topic1")
    info2 = recorder.get_subscriptions_info_by_topic("/test/topic2")
    info3 = recorder.get_subscriptions_info_by_topic("/other/topic")
    assert (
        info1[0].topic_type == "std_msgs/msg/String"
    ), "Recorder is not subscribed to /test/topic1"
    assert (
        info2[0].topic_type == "std_msgs/msg/String"
    ), "Recorder is not subscribed to /test/topic2"
    assert (
        len(info3) == 0
    ), "Recorder is subscribed to /other/topic when it should not be"

    publisher.destroy_node()
    recorder.destroy_node()
