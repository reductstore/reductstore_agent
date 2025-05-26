import asyncio
import io

import rclpy
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from rclpy.node import Node
from rclpy.parameter import Parameter
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
                "incremental",
            ),
        ]
    )

    for _ in range(2):
        # Publish and receive messages
        for i in range(3):
            msg = String()
            msg.data = f"test_data_actual_upload_{i}"
            publisher.publish(msg)
            rclpy.spin_once(publisher_node, timeout_sec=0.2)
            rclpy.spin_once(recorder, timeout_sec=0.2)

        # Wait the timer to trigger the upload
        rclpy.spin_once(recorder, timeout_sec=2.0)

    async def check_reduct_data():
        data_all = []
        bucket = await reduct_client.get_bucket("test_bucket")
        async for record in bucket.query("timer_test_actual"):
            data_all.append(await record.read_all())
        return data_all

    loop = asyncio.get_event_loop()
    data_all = loop.run_until_complete(check_reduct_data())
    assert len(data_all) > 0, "No data found in ReductStore for the uploaded messages"
    assert len(data_all) == 2, "Expected exactly one record in ReductStore"

    for j, data in enumerate(data_all):
        reader = make_reader(io.BytesIO(data), decoder_factories=[DecoderFactory()])
        message_count = reader.get_summary().statistics.message_count
        assert (
            message_count == 3
        ), f"Expected 3 messages in record {j}, found {message_count}"
        for i, (schema_, channel_, message_, ros2_msg) in enumerate(
            reader.iter_decoded_messages()
        ):
            # Check the schema
            assert (
                "string data" in schema_.data.decode()
            ), f"[{i}] Message type mismatch"
            assert schema_.id == 1, f"[{i}] Schema ID should be 1"
            assert schema_.encoding == "ros2msg", f"[{i}] Encoding should be 'ros2msg'"
            assert (
                schema_.name == "std_msgs/msg/String"
            ), f"[{i}] Schema name should be 'std_msgs/msg/String'"

            # Check the channel
            assert (
                channel_.topic == "/test/topic"
            ), f"[{i}] Topic mismatch in uploaded data"
            assert (
                channel_.message_encoding == "cdr"
            ), f"[{i}] Message encoding should be 'cdr'"
            assert channel_.schema_id == 1, f"[{i}] Schema ID should be 1"

            # Check the message
            assert (
                ros2_msg.data == f"test_data_actual_upload_{i}"
            ), f"[{i}] Data mismatch"
            assert message_.channel_id == 1, f"[{i}] Channel ID  should be 1"
            assert message_.log_time > 0, f"[{i}] Log time should be greater than 0"
            assert (
                message_.publish_time > 0
            ), f"[{i}] Publish time should be greater than 0"
            assert (
                message_.publish_time < message_.log_time
            ), f"[{i}] Publish time should be less than or equal to log time"

    recorder.destroy_node()
    publisher_node.destroy_node()
