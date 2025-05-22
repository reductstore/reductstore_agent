import asyncio
import io
import time as pytime

import rclpy
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.serialization import deserialize_message
from std_msgs.msg import String

from ros2_reduct_agent.recorder import Recorder


def test_recorder_timer_trigger_actual_upload(reduct_client):
    """Test that the Recorder uploads to ReductStore and the data is retrievable."""
    publisher_node = Node("test_publisher_actual_upload")
    publisher = publisher_node.create_publisher(String, "/test/topic", 10)

    recorder = Recorder(
        parameter_overrides=[
            Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
            Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
            Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
            Parameter(
                "pipelines.timer_test_actual.include_topics",
                Parameter.Type.STRING_ARRAY,
                ["/test/topic"],
            ),
            Parameter(
                "pipelines.timer_test_actual.split.max_duration_s",
                Parameter.Type.INTEGER,
                1,
            ),
            Parameter(
                "pipelines.timer_test_actual.filename_mode",
                Parameter.Type.STRING,
                "counter",
            ),
        ]
    )

    msg = String()
    msg.data = "test_data_actual_upload"
    publisher.publish(msg)

    # Puplish messages until timer trigger
    for _ in range(5):
        rclpy.spin_once(recorder, timeout_sec=0.2)
        rclpy.spin_once(publisher_node, timeout_sec=0.2)

    async def check_reduct_data():
        bucket = await reduct_client.get_bucket("test_bucket")

        async for record in bucket.query("timer_test_actual"):
            return await record.read_all()

    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(check_reduct_data())
    assert data is not None, "No data found in ReductStore for the uploaded segment"

    reader = make_reader(io.BytesIO(data), decoder_factories=[DecoderFactory()])
    for schema_, channel_, message_, ros2_msg in reader.iter_decoded_messages():
        assert channel_.topic == "/test/topic", "Topic mismatch in uploaded data"
        assert (
            schema_.data.decode() == "# std_msgs/String\nstring data"
        ), "Message type mismatch in uploaded data"
        assert (
            ros2_msg.data == "test_data_actual_upload"
        ), "Message content mismatch in uploaded data"

    recorder.destroy_node()
    publisher_node.destroy_node()
