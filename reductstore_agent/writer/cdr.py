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

"""CDR output format logic per pipeline."""


from typing import Any, Dict

from rclpy.serialization import serialize_message
from reduct import Batch, Bucket

from ..utils import get_or_create_event_loop, metadata_size, ns_to_us
from .base import OutputWriter
from ..dynamic_labels import LabelStateTracker

KB_100 = 100 * 1024
BATCH_MAX_METADATA_SIZE = 8 * 1024
BATCH_MAX_RECORDS = 85


class CdrOutputWriter(OutputWriter):
    """CDROutput Class for binary CDR."""

    def __init__(
        self,
        bucket: Bucket,
        pipeline_name: str,
        flush_threshold_bytes: int = 5 * 1024 * 1024,  # i.e. 2MB
        logger=None,
        label_tracker: LabelStateTracker | None = None
    ):
        """Initialize CDROutput writer."""
        self.bucket = bucket
        self.pipeline_name = pipeline_name
        self._flush_threshold_bytes = flush_threshold_bytes
        self._batch = Batch()
        self._batch_size_bytes: int = 0
        self._is_flushing = False
        self._batch_metadata_size: int = 0
        self.label_tracker = label_tracker

        self._topic_to_msg_type: Dict[str, str] = {}
        if logger is None:
            from rclpy.logging import get_logger

            self.logger = get_logger(f"CdrOutputWriter[{pipeline_name}]")
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
            labels=labels,
        )

    def write_message(self, message: Any, publish_time: int, topic: str, **kwargs):
        """Write message to batch - synchronous interface."""
        if self.label_tracker is not None:
            self.label_tracker.update(topic, message)

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

        loop = get_or_create_event_loop()
        # Stream message if larger than 100KB
        if len(serialized_data) >= KB_100:
            self.logger.info("shouldnt be here")

            async def upload_large():
                await self.flush_and_upload_batch()
                await self.upload_to_reductstore(serialized_data, timestamp_us, labels)

            loop.run_until_complete(upload_large())

        # If smaller than 100KB batch the record
        self.append_record(timestamp_us, serialized_data, labels)

        if self._batch_size_bytes >= self._flush_threshold_bytes:
            self.logger.info("Threshold triggered")
            loop.run_until_complete(self.flush_and_upload_batch())

        elif len(self._batch) >= BATCH_MAX_RECORDS:
            self.logger.info("Exceeded max records of batch. Uploading batch.")
            loop.run_until_complete(self.flush_and_upload_batch())

        elif self._batch_metadata_size >= BATCH_MAX_METADATA_SIZE:
            self.logger.info("Exceeded metadata threshold. Uploading batch.")
            loop.run_until_complete(self.flush_and_upload_batch())

    def append_record(
        self, timestamp_us: int, serialized_data: bytes, labels: Dict
    ) -> None:
        """Append raw data to batch."""
        self._batch_size_bytes += len(serialized_data)
        self._batch_metadata_size += metadata_size(labels)
        self._batch.add(timestamp=timestamp_us, data=serialized_data, labels=labels)

    async def flush_and_upload_batch(self) -> None:
        """Flush the batch and upload to ReductStore."""
        if self._batch_size_bytes == 0:
            return
        if self._is_flushing:
            self.logger.info("Already flushing. Returning ...")
            return

        self._is_flushing = True

        try:
            batch_to_send = self._batch
            self._batch = Batch()
            self._batch_size_bytes = 0
            errors = await self.bucket.write_batch(
                entry_name=self.pipeline_name, batch=batch_to_send
            )
            if errors:
                self.logger.warning("Error with writing batch to bucket.")
            else:
                self.logger.info("Uploaded batch.")
        except Exception as exc:
            self.logger.error(f"[{self.pipeline_name}]Failed to upload batch: {exc}")
        finally:
            self._is_flushing = False

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register message schema."""
        self._topic_to_msg_type[topic_name] = msg_type_str

    def flush_on_shutdown(self):
        """Flush the batch on shutdown."""
        loop = get_or_create_event_loop()
        loop.run_until_complete(self.flush_and_upload_batch())

    @property
    def size(self) -> int:
        """Get current batch size for compatibility with MCAP writer."""
        return self._batch_size_bytes
