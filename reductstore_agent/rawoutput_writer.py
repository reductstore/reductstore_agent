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


from typing import Any, Dict, Union

from rclpy.serialization import serialize_message
from reduct import Bucket

from .utils import ns_to_us, ros2_type_name

KB_100 = 100 * 1024


class RawOutputWriter:
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
        self._batch: Dict[int, Dict[str, Union[bytes, Dict[str, str]]]] = {}
        self._batch_bytes: int = 0
        self.pipeline_name = pipeline_name
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

    async def write_message(self, message: Any, publish_time: int):
        """Write message to batch or upload to ReductStore."""
        try:
            serialized_data = serialize_message(message)
        except Exception as exc:
            self.logger.warning(
                f"[{self.pipeline_name}] Could not serialize message "
                f"({message.__class__.__name__}): {exc}"
            )
            return

        timestamp_us = ns_to_us(publish_time)
        topic_type = ros2_type_name(message)
        labels = {
            "type": f"{topic_type} Message",
            "serialization": "cdr",
        }
        if len(serialized_data) >= KB_100:
            await self.flush_batch()
            await self.upload_to_reductstore(serialized_data, timestamp_us, labels)
            return

        # If smaller than 100KB batch the record
        self.append_record(timestamp_us, serialized_data, labels)
        if self._batch_bytes >= self.flush_threshold_bytes:
            await self.flush_batch()

    def append_record(
        self, timestamp_us: int, serialized_data: bytes, labels: Dict
    ) -> None:
        """Append raw data to batch."""
        self._batch[timestamp_us] = {"data": serialized_data, "labels": labels}
        self._batch_bytes += len(serialized_data)

    async def flush_batch(self) -> None:
        """Flush the batch and upload to ReductStore."""
        errors = await self.bucket.write_batch(
            entry_name=self.pipeline_name, batch=self._batch
        )
        if errors:
            self.logger.warning(
                f"[{self.pipeline_name}] Batch upload failed for {len(errors)} record."
                f"Keys: {list(errors.keys())[:5]}..."
            )
        self._batch.clear()
        self._batch_bytes = 0
