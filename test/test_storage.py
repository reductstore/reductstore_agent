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


import io

import rclpy
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from std_msgs.msg import String

from ros2_reduct_agent.utils import get_or_create_event_loop


def publish_and_spin(publisher_node, publisher, recorder, n_msgs=3, n_cycles=2):
    """Publish messages and spin the nodes to allow the recorder to process them."""
    for _ in range(n_cycles):
        for i in range(n_msgs):
            msg = String()
            msg.data = f"test_data_{i}"
            publisher.publish(msg)
            rclpy.spin_once(publisher_node, timeout_sec=0.2)
            rclpy.spin_once(recorder, timeout_sec=0.2)
        # Spin recorder long enough for timer to trigger
        rclpy.spin_once(recorder, timeout_sec=2.0)


def test_timer_trigger_uploads_to_bucket(
    reduct_client, publisher_node, publisher, basic_recorder
):
    """Test that the timer triggers and uploads data to the bucket."""
    publish_and_spin(publisher_node, publisher, basic_recorder)

    async def fetch_all():
        bucket = await reduct_client.get_bucket("test_bucket")
        output = []
        async for record in bucket.query("timer_test_topic"):
            output.append(await record.read_all())
        return output

    data_blobs = get_or_create_event_loop().run_until_complete(fetch_all())

    assert len(data_blobs) == 2, f"got {len(data_blobs)} files, expected 2"


def test_uploaded_blob_has_expected_message_count(
    reduct_client, publisher_node, publisher, basic_recorder
):
    """Test that the uploaded blob has the expected number of messages."""
    publish_and_spin(publisher_node, publisher, basic_recorder)

    async def fetch_one():
        bucket = await reduct_client.get_bucket("test_bucket")
        async for record in bucket.query("timer_test_topic"):
            return await record.read_all()
        return None

    blob = get_or_create_event_loop().run_until_complete(fetch_one())
    assert blob is not None, "no upload found"

    reader = make_reader(io.BytesIO(blob), decoder_factories=[DecoderFactory()])
    stats = reader.get_summary().statistics
    assert stats.message_count == 3, f"expected 3 messages, got {stats.message_count}"


def test_uploaded_messages_have_correct_schema_and_content(
    reduct_client, publisher_node, publisher, basic_recorder
):
    """Test that the uploaded messages have the correct schema and content."""
    publish_and_spin(publisher_node, publisher, basic_recorder)

    async def fetch_one():
        bucket = await reduct_client.get_bucket("test_bucket")
        async for record in bucket.query("timer_test_topic"):
            return await record.read_all()
        return None

    blob = get_or_create_event_loop().run_until_complete(fetch_one())
    reader = make_reader(io.BytesIO(blob), decoder_factories=[DecoderFactory()])

    for idx, (schema, channel, msg_meta, ros2_msg) in enumerate(
        reader.iter_decoded_messages()
    ):
        # schema checks
        assert schema.id == 1
        assert schema.name == "std_msgs/msg/String"
        assert schema.encoding == "ros2msg"
        assert b"string data" in schema.data

        # channel checks
        assert channel.schema_id == 1
        assert channel.topic == "/test/topic"
        assert channel.message_encoding == "cdr"

        # message checks
        assert msg_meta.channel_id == 1
        assert msg_meta.publish_time > 0
        assert msg_meta.log_time > 0
        assert msg_meta.publish_time <= msg_meta.log_time
        assert ros2_msg.data == f"test_data_{idx}"


def test_parallel_pipelines_upload_both_topics(
    reduct_client, publisher_node, publisher, parallel_recorder
):
    """Test that parallel pipelines upload both /test/topic and /rosout."""
    msg = String()
    msg.data = "parallel_test"
    for _ in range(5):
        publisher.publish(msg)
        rclpy.spin_once(publisher_node, timeout_sec=0.2)
        # Spin recorder twice to allow both pipelines to process
        rclpy.spin_once(parallel_recorder, timeout_sec=0.2)
        rclpy.spin_once(parallel_recorder, timeout_sec=0.2)

    # Wait for the timer to trigger and upload
    rclpy.spin_once(parallel_recorder, timeout_sec=2.0)

    async def fetch_both():
        bucket = await reduct_client.get_bucket("test_bucket")
        output = {"timer_test_topic": [], "timer_rosout": []}
        async for rec in bucket.query("timer_test_topic"):
            output["timer_test_topic"].append(await rec.read_all())
        async for rec in bucket.query("timer_rosout"):
            output["timer_rosout"].append(await rec.read_all())
        return output

    data = get_or_create_event_loop().run_until_complete(fetch_both())
    assert data["timer_test_topic"], "no /test/topic uploads"
    assert data["timer_rosout"], "no /rosout uploads"

    # Check /test/topic
    reader = make_reader(
        io.BytesIO(data["timer_test_topic"][0]), decoder_factories=[DecoderFactory()]
    )
    assert reader.get_summary().statistics.message_count >= 1
    for schema, channel, _, _ in reader.iter_decoded_messages():
        assert schema.name == "std_msgs/msg/String"
        assert channel.topic == "/test/topic"

    # Check /rosout
    reader2 = make_reader(
        io.BytesIO(data["timer_rosout"][0]), decoder_factories=[DecoderFactory()]
    )
    assert reader2.get_summary().statistics.message_count >= 1
    for schema, channel, _, _ in reader2.iter_decoded_messages():
        assert schema.name == "rcl_interfaces/msg/Log"
        assert channel.topic == "/rosout"
