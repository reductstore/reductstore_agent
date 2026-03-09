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

"""Unit tests for attachment handling logic."""

import json
from unittest.mock import AsyncMock, Mock

from reductstore_agent.attachment import AttachmentHandler


def test_serialize_attachment_data():
    """Test the _serialize_attachment_data method of AttachmentHandler."""
    data_bytes = b"binary data"
    assert AttachmentHandler._serialize_attachment_data(data_bytes) == data_bytes

    data_str = "string data"
    assert AttachmentHandler._serialize_attachment_data(data_str) == data_str.encode(
        "utf-8"
    )

    data_dict = {"key": "value"}
    expected_json = b'{"key":"value"}'
    assert AttachmentHandler._serialize_attachment_data(data_dict) == expected_json

    data_list = [1, 2, 3]
    expected_json_list = b"[1,2,3]"
    assert AttachmentHandler._serialize_attachment_data(data_list) == expected_json_list


def test_build_ros_payload():
    """Test that ROS payload is created and cached in attachment map."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    payload = handler.build_ros_payload(
        msg_type_str="std_msgs/msg/String",
        topic="/test/topic",
        schema="string data",
    )
    assert payload == {
        "encoding": AttachmentHandler.ROS_ENCODING,
        "topic": "/test/topic",
        "schema": "string data",
    }

    assert AttachmentHandler.ROS_ATTACHMENT_NAME in handler.attachments
    stored_payload = json.loads(
        handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME].decode("utf-8")
    )
    assert stored_payload == payload


def test_build_ros_payload_has_only_required_keys():
    """ROS payload should contain only encoding, topic, and schema."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    payload = handler.build_ros_payload(
        msg_type_str="std_msgs/msg/String",
        topic="/test/topic",
        schema="string data",
    )
    assert set(payload.keys()) == {"encoding", "topic", "schema"}
    assert "msg_type" not in payload


def test_build_ros_payload_updates_cached_ros_attachment():
    """Building payload again should update cached '$ros' attachment bytes."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    handler.build_ros_payload(
        msg_type_str="std_msgs/msg/String",
        topic="/test/topic",
        schema="schema-v1",
    )
    first_payload = handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME]

    handler.build_ros_payload(
        msg_type_str="std_msgs/msg/String",
        topic="/test/topic",
        schema="schema-v2",
    )
    second_payload = handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME]
    assert first_payload != second_payload
    assert json.loads(second_payload.decode("utf-8"))["schema"] == "schema-v2"


def test_upload_attachments_uses_json_compatible_payload():
    """Upload path should send nested '$ros' payload as JSON-safe dict."""
    bucket = Mock()
    bucket.write_attachments = AsyncMock()
    handler = AttachmentHandler(bucket=bucket, pipeline_name="test_pipeline")

    import asyncio

    asyncio.run(
        handler.upload_attachments(
            "entry",
            {AttachmentHandler.ROS_ATTACHMENT_NAME: b'{"encoding":"cdr"}'},
        )
    )

    _, kwargs = bucket.write_attachments.call_args
    assert kwargs["entry_name"] == "entry"
    assert kwargs["attachments"] == {
        AttachmentHandler.ROS_ATTACHMENT_NAME: '{"encoding":"cdr"}'
    }
