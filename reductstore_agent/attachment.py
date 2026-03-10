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

"""Attachment handling logic."""

import base64
import json
from typing import Any


class AttachmentHandler:
    """Class to handle attachments."""

    ROS_ATTACHMENT_NAME = "$ros"
    ROS_ENCODING = "cdr"

    def __init__(self, bucket, pipeline_name, logger=None):
        """Initialize AttachmentHandler."""
        self.bucket = bucket
        self.pipeline_name = pipeline_name
        self.attachments: dict[str, bytes] = {}

        if logger is None:
            from rclpy.logging import get_logger

            self.logger = get_logger("AttachmentHandler")
        else:
            self.logger = logger

    @staticmethod
    def _serialize_attachment_data(data: Any) -> bytes:
        """Serialize attachment payload to bytes."""
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")

        def default_encoder(obj):
            if isinstance(obj, (bytes, bytearray, memoryview)):
                return bytes(obj).decode("utf-8")
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable"
            )

        return json.dumps(
            data,
            separators=(",", ":"),
            ensure_ascii=True,
            default=default_encoder,
        ).encode("utf-8")

    async def upload_attachments(self, entry_name: str, attachments: dict[str, Any]):
        """Upload attachment(s) to ReductStore."""
        self.logger.debug(
            f"Uploading attachments {list(attachments.keys())} for entry '{entry_name}'"
        )
        await self.bucket.write_attachments(
            entry_name=entry_name,
            attachments=attachments,
        )

    def build_ros_payload(self, topic: str, schema: str) -> dict:
        """Build and cache metadata payload for '$ros' attachment."""
        payload = {
            "encoding": self.ROS_ENCODING,
            "topic": topic,
            "schema_base64": self.schema_base64_converter(schema),
        }
        converted_schema = self.schema_converter(schema)
        if converted_schema is not None:
            payload["schema"] = converted_schema
        self.attachments[self.ROS_ATTACHMENT_NAME] = self._serialize_attachment_data(
            payload
        )
        return payload

    def schema_converter(self, schema: str) -> str | None:
        """Convert schema to readable UTF-8 text when possible."""
        if isinstance(schema, (bytes, bytearray, memoryview)):
            try:
                return bytes(schema).decode("utf-8")
            except UnicodeDecodeError:
                return None
        return str(schema)

    def schema_base64_converter(self, schema: str) -> str:
        """Convert schema to base64 for unambiguous transport."""
        if isinstance(schema, str):
            schema = schema.encode("utf-8")
        return base64.b64encode(bytes(schema)).decode("ascii")

    async def ensure_ros_attachment(
        self,
        entry_name: str,
        topic: str,
        schema: str,
    ) -> None:
        """Upload '$ros' attachment for this entry, overwriting any existing value."""
        payload = self.build_ros_payload(
            topic=topic,
            schema=schema,
        )
        await self.upload_attachments(
            entry_name,
            {self.ROS_ATTACHMENT_NAME: payload},
        )
        self.logger.info(
            f"Uploaded '$ros' attachment for entry '{entry_name}'."
        )
