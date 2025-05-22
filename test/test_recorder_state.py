import pytest
import rclpy
from rclpy.node import Node
from rclpy.parameter import Parameter
from std_msgs.msg import String

from ros2_reduct_agent.recorder import Recorder


def generate_string(size_kb: int) -> str:
    """Generate a string of size_kb kilobytes."""
    return "X" * (size_kb * 1024)


@pytest.mark.parametrize("size_kb", [1, 10, 100])
def test_recorder_state_size(size_kb: int):
    """Test that the Recorder node can handle large messages."""
    publisher_node = Node("test_publisher")
    publisher = publisher_node.create_publisher(String, "/test/topic", 10)

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
                "pipelines.test.split.max_duration_s", Parameter.Type.INTEGER, 3600
            ),
            Parameter("pipelines.test.filename_mode", Parameter.Type.STRING, "counter"),
        ]
    )

    msg = String()
    msg.data = generate_string(size_kb)

    for i in range(5):
        publisher.publish(msg)

    for _ in range(5):
        rclpy.spin_once(recorder, timeout_sec=0.1)
        rclpy.spin_once(publisher_node, timeout_sec=0.1)

    # Size of 5 messages of size_kb KB each + 45 bytes for the header
    assert (
        recorder.pipeline_states["test"].current_size == 5 * size_kb * 1024 + 45
    ), "Recorder did not receive the expected number of large messages"

    recorder.destroy_node()
    publisher_node.destroy_node()


def test_recorder_timer_trigger(monkeypatch):
    """Test that the Recorder triggers segment reset on timer expiration."""
    uploads = []

    def mock_upload_mcap(_, pipeline_name, data, timestamp):
        uploads.append((pipeline_name, data, timestamp))

    monkeypatch.setattr(Recorder, "upload_mcap", mock_upload_mcap)

    recorder = Recorder(
        parameter_overrides=[
            Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
            Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
            Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
            Parameter(
                "pipelines.timer_test.include_topics",
                Parameter.Type.STRING_ARRAY,
                ["/test/topic"],
            ),
            Parameter(
                "pipelines.timer_test.split.max_duration_s", Parameter.Type.INTEGER, 1
            ),
            Parameter(
                "pipelines.timer_test.filename_mode", Parameter.Type.STRING, "counter"
            ),
        ]
    )

    # Wait for the timer to trigger
    rclpy.spin_once(recorder, timeout_sec=1.1)

    assert len(uploads) == 1, "Timer did not trigger upload as expected"
    assert (
        uploads[0][0] == "timer_test"
    ), "Pipeline name in upload does not match expected"
    assert (
        recorder.pipeline_states["timer_test"].counter == 1
    ), "Pipeline segment was not set as expected"

    recorder.destroy_node()
