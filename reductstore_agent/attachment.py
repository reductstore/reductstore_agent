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

    @staticmethod
    def _json_compatible(data: Any) -> Any:
        """Convert payload to JSON-compatible values for write_attachments."""
        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            try:
                return raw.decode("utf-8")
            except UnicodeDecodeError:
                return base64.b64encode(raw).decode("ascii")
        if isinstance(data, dict):
            return {
                str(key): AttachmentHandler._json_compatible(value)
                for key, value in data.items()
            }
        if isinstance(data, (list, tuple, set)):
            return [AttachmentHandler._json_compatible(value) for value in data]
        return data

    async def upload_attachment(self, entry_name: str, attachments: dict[str, Any]):
        """Upload attachment(s) to ReductStore."""
        await self.upload_attachments(entry_name, attachments)

    async def upload_attachments(self, entry_name: str, attachments: dict[str, Any]):
        """Upload attachment(s) to ReductStore."""
        self.logger.debug(
            f"Uploading attachments {list(attachments.keys())} for entry '{entry_name}'"
        )
        await self.bucket.write_attachments(
            entry_name=entry_name,
            attachments=self._json_compatible(attachments),
        )

    async def check_ros_attachment(self, entry_name: str) -> bool:
        """Check if reserved '$ros' attachment exists for the entry."""
        try:
            attachments = await self.bucket.read_attachments(entry_name)
        except Exception as exc:
            self.logger.warning(
                f"[{self.pipeline_name}] Failed to read attachments for "
                f"entry '{entry_name}': {exc}"
            )
            return False

        exists = False
        if isinstance(attachments, dict):
            # Newer clients return the ROS payload directly.
            if {"encoding", "topic", "schema"}.issubset(attachments.keys()):
                exists = True
            else:
                exists = self.ROS_ATTACHMENT_NAME in attachments
        elif isinstance(attachments, (list, tuple, set)):
            names: set[str] = set()
            for item in attachments:
                if isinstance(item, str):
                    names.add(item)
                elif hasattr(item, "name"):
                    names.add(item.name)
            exists = self.ROS_ATTACHMENT_NAME in names

        if exists:
            self.logger.debug(f"ROS attachment found for entry '{entry_name}'")
            return True

        self.logger.debug(f"No ROS attachment found for entry '{entry_name}'")
        return False

    def build_ros_payload(self, msg_type_str: str, topic: str, schema: str) -> dict:
        """Build and cache metadata payload for '$ros' attachment."""
        # msg_type_str is kept in signature for backward compatibility with callers.
        _ = msg_type_str
        converted_schema = self.schema_converter(msg_type_str, schema)
        payload = {
            "encoding": self.ROS_ENCODING,
            "topic": topic,
            "schema": converted_schema,
        }
        self.attachments[self.ROS_ATTACHMENT_NAME] = self._serialize_attachment_data(
            payload
        )
        return payload

    def schema_converter(self, msg_type_str: str, schema: str) -> str:
        """Convert schema to be JSON deserializable."""
        if isinstance(schema, (bytes, bytearray, memoryview)):
            try:
                return bytes(schema).decode("utf-8")
            except Exception:
                return base64.b64encode(bytes(schema)).decode("utf-8")
        return str(schema)

    async def ensure_ros_attachment(
        self,
        entry_name: str,
        msg_type_str: str,
        topic: str,
        schema: str,
    ) -> bool:
        """Upload '$ros' attachment only if it is missing for this entry."""
        if await self.check_ros_attachment(entry_name):
            return False

        payload = self.build_ros_payload(
            msg_type_str=msg_type_str,
            topic=topic,
            schema=schema,
        )
        await self.upload_attachments(
            entry_name,
            {self.ROS_ATTACHMENT_NAME: payload},
        )
        self.logger.info(f"Uploaded '$ros' attachment for entry '{entry_name}'")
        return True
