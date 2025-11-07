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

"""Raw output format logic per pipeline."""


from typing import Any, Dict

from rclpy.serialization import serialize_message
from reduct import Batch, Bucket

from ..utils import get_or_create_event_loop, ns_to_us
from .base import OutputWriter

KB_100 = 100 * 1024


class RawOutputWriter(OutputWriter):
    """RawOutput Class for binary CDR."""

    def __init__(
        self,
        bucket: Bucket,
        pipeline_name: str,
        flush_threshold_bytes: int = 10 * 1024 * 1024,  # i.e. 10MB
        logger=None,
    ):
        """Initialize RawOutput writer."""
        self.bucket = bucket
        self.pipeline_name = pipeline_name
        self.flush_threshold_bytes = flush_threshold_bytes
        self._batch = Batch()
        self._batch_size: int = 0
        self.pipeline_name = pipeline_name

        self._topic_to_msg_type: Dict[str, str] = {}
        if logger is None:
            from rclpy.logging import get_logger

            self.logger = get_logger(f"RawOutputWriter[{pipeline_name}]")
        else:
            self.logger = logger

    async def upload_to_reductstore(
        self, serialized_data: bytes, timestamp_us: int, labels: Dict
    ):
        """Upload raw data to ReductStore with labels and timestamp."""
        await self.bucket.write(
            entry_name=self.pipeline_name,
            timestamp=timestamp_us,
            data=serialized_data,
            content_length=len(serialized_data),
            labels=labels,
        )

    def write_message(self, message: Any, publish_time: int, topic: str, **kwargs):
        """Write message to batch - synchronous interface."""
        try:
            serialized_data = serialize_message(message)
        except Exception as exc:
            if self.logger:
                self.logger.warning(
                    f"[{self.pipeline_name}] Could not serialize message "
                    f"({message.__class__.__name__}): {exc}"
                )
            return

        timestamp_us = ns_to_us(publish_time)

        msg_type_str = self._topic_to_msg_type.get(topic, "unknown")
        labels = {
            "type": msg_type_str,
            "topic": topic,
            "serialization": "cdr",
        }

        # For large messages, trigger immediate upload
        if len(serialized_data) >= KB_100:

            async def upload_large():
                await self.flush_and_upload_batch()
                await self.upload_to_reductstore(serialized_data, timestamp_us, labels)

            loop = get_or_create_event_loop()
            loop.create_task(upload_large())
            return

        # If smaller than 100KB batch the record
        self.append_record(timestamp_us, serialized_data, labels)
        if self.size >= self.flush_threshold_bytes:
            loop = get_or_create_event_loop()
            loop.create_task(self.flush_and_upload_batch())

    def append_record(
        self, timestamp_us: int, serialized_data: bytes, labels: Dict
    ) -> None:
        """Append raw data to batch."""
        self._batch_size += len(serialized_data)
        self._batch.add(timestamp=timestamp_us, data=serialized_data, labels=labels)

    async def flush_and_upload_batch(self) -> None:
        """Flush the batch and upload to ReductStore."""
        if self._batch_size == 0 and not self._batch:
            return

        errors = await self.bucket.write_batch(
            entry_name=self.pipeline_name, batch=self._batch
        )
        if errors:
            self.logger.warning(
                f"[{self.pipeline_name}] Batch upload failed for {len(errors)} record."
                f"Keys: {list(errors.keys())[:5]}..."
            )
        self._batch = Batch()
        self._batch_size = 0

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register message schema."""
        self._topic_to_msg_type[topic_name] = msg_type_str

    @property
    def size(self) -> int:
        """Get current batch size for compatibility with MCAP writer."""
        return self._batch_size
