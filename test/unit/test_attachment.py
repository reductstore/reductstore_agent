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

import base64
import json

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
        topic="/test/topic",
        schema="string data",
        schema_name="std_msgs/msg/String",
    )
    assert payload == {
        "encoding": AttachmentHandler.ROS_ENCODING,
        "topic": "/test/topic",
        "schema_name": "std_msgs/msg/String",
        "schema": "string data",
        "schema_base64": base64.b64encode(b"string data").decode("ascii"),
    }

    assert AttachmentHandler.ROS_ATTACHMENT_NAME in handler.attachments
    stored_payload = json.loads(
        handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME].decode("utf-8")
    )
    assert stored_payload == payload


def test_build_ros_payload_has_only_required_keys():
    """ROS payload should include the expected ROS attachment fields."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    payload = handler.build_ros_payload(
        topic="/test/topic",
        schema="string data",
        schema_name="std_msgs/msg/String",
    )
    assert set(payload.keys()) == {
        "encoding",
        "topic",
        "schema_name",
        "schema",
        "schema_base64",
    }


def test_build_ros_payload_updates_cached_ros_attachment():
    """Building payload again should update cached '$ros' attachment bytes."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    handler.build_ros_payload(
        topic="/test/topic",
        schema="schema-v1",
        schema_name="pkg/msg/V1",
    )
    first_payload = handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME]

    handler.build_ros_payload(
        topic="/test/topic",
        schema="schema-v2",
        schema_name="pkg/msg/V2",
    )
    second_payload = handler.attachments[AttachmentHandler.ROS_ATTACHMENT_NAME]
    assert first_payload != second_payload
    decoded_payload = json.loads(second_payload.decode("utf-8"))
    assert decoded_payload["schema"] == "schema-v2"
    assert decoded_payload["schema_name"] == "pkg/msg/V2"


def test_build_ros_payload_omits_text_schema_for_non_utf8_bytes():
    """Binary schemas should use schema_base64 without a misleading text schema."""
    handler = AttachmentHandler(bucket=None, pipeline_name="test_pipeline")
    payload = handler.build_ros_payload(
        topic="/test/topic",
        schema=b"\xff\x00\x01",
        schema_name="pkg/msg/Binary",
    )
    assert payload == {
        "encoding": AttachmentHandler.ROS_ENCODING,
        "topic": "/test/topic",
        "schema_name": "pkg/msg/Binary",
        "schema_base64": "/wAB",
    }
