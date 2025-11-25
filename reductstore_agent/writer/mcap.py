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

"""MCAP output format logic per pipeline."""

from tempfile import SpooledTemporaryFile
from typing import Any, AsyncGenerator, Dict

from mcap.records import Schema
from mcap_ros2.writer import Writer as McapWriter
from reduct import Bucket
from rosbag2_py import LocalMessageDefinitionSource

from ..dynamic_labels import LabelStateTracker
from ..models import FilenameMode, PipelineConfig
from ..utils import get_or_create_event_loop
from .base import OutputWriter


class McapOutputWriter(OutputWriter):
    """MCAP Output Writer for handling MCAP file creation and upload."""

    def __init__(
        self,
        bucket: Bucket,
        pipeline_name: str,
        config: PipelineConfig,
        logger=None,
        label_tracker: LabelStateTracker | None = None,
    ):
        """Initialize MCAP writer."""
        self.bucket = bucket
        self.pipeline_name = pipeline_name
        self.config = config
        self._increment = 0
        self.first_timestamp: int | None = None
        self.current_size = 0
        self._is_uploading = False
        self.label_tracker = label_tracker

        # Schema tracking
        self.schema_by_type: Dict[str, Schema] = {}
        self.schemas_by_topic: Dict[str, Schema] = {}

        # Initialize _buffer and writer
        self._buffer: SpooledTemporaryFile[bytes] | None = None
        self.writer: McapWriter | None = None
        self._init_writer()

        if logger is None:
            from rclpy.logging import get_logger

            self.logger = get_logger(f"McapOutputWriter[{pipeline_name}]")
        else:
            self.logger = logger

    def _init_writer(self):
        """Initialize a new MCAP writer and buffer."""
        max_size = self.config.spool_max_size_bytes
        self._buffer = SpooledTemporaryFile(max_size=max_size, mode="w+b")
        self.writer = McapWriter(
            self._buffer,
            chunk_size=self.config.chunk_size_bytes,
            compression=self.config.compression,
            enable_crcs=self.config.enable_crcs,
        )
        self.current_size = 0
        self.first_timestamp = None
        self._is_uploading = False

        # Clear schemas (they need to be re-registered for new writer)
        self.schema_by_type.clear()
        self.schemas_by_topic.clear()

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema for a message type and associate it with the topic."""
        if topic_name in self.schemas_by_topic:
            return  # Already registered

        if msg_type_str in self.schema_by_type:
            schema = self.schema_by_type[msg_type_str]
        else:
            source = LocalMessageDefinitionSource()
            msg_def = source.get_full_text(msg_type_str)
            schema = self.writer.register_msgdef(
                datatype=msg_def.topic_type,
                msgdef_text=msg_def.encoded_message_definition,
            )
            self.schema_by_type[msg_type_str] = schema
            self.logger.debug(
                f"[{topic_name}] Registered schema for message type '{msg_type_str}'"
            )

        self.schemas_by_topic[topic_name] = schema

    def write_message(
        self,
        message: Any,
        publish_time: int,
        topic: str,
        **kwargs,
    ):
        """Write message to MCAP file synchronously."""
        if self.first_timestamp is None:
            self.first_timestamp = publish_time

        # Process each message and update labels
        if self.label_tracker is not None:
            self.label_tracker.update(topic, message)

        # Get the actual schema from our registry
        actual_schema = self.schemas_by_topic.get(topic)
        if not actual_schema:
            if self.logger:
                self.logger.warning(
                    f"[{self.pipeline_name}] No schema registered for topic '{topic}' "
                    "- skipping message"
                )
            return

        # Write to MCAP (internal buffer)
        self.writer.write_message(
            topic=topic,
            schema=actual_schema,
            message=message,
            publish_time=publish_time,
        )

        self.current_size = self._buffer.tell()

        # Check if we need to split due to size limit
        if (
            self.config.split_max_size_bytes
            and self.current_size >= self.config.split_max_size_bytes
        ):

            loop = get_or_create_event_loop()
            loop.create_task(self.finish_and_upload())

    async def finish_and_upload(self):
        """Finish current MCAP, upload it, and reset the writer."""
        if self._is_uploading:
            self.logger.warning(
                (
                    f"[{self.pipeline_name}] Upload already in progress"
                    " - skipping upload."
                )
            )
            return

        if self.current_size == 0:
            self.logger.info(
                f"[{self.pipeline_name}] No new data since last upload"
                " - skipping upload."
            )
            return

        # Prevent concurrent uploads
        self._is_uploading = True

        try:
            # Determine file index
            filename_mode = self.config.filename_mode
            file_index = (
                self._increment
                if filename_mode == FilenameMode.INCREMENTAL
                else (self.first_timestamp or 0) // 1_000  # Convert to microseconds
            )

            # Finish the MCAP writer
            self.writer.finish()

            # Upload the buffer
            await self._upload_to_reductstore(file_index)

            self.logger.info(
                (
                    f"[{self.pipeline_name}] MCAP uploaded successfully"
                    f" (size: {self.current_size} bytes)"
                )
            )

            # Increment for next segment
            self._increment += 1

        except Exception as exc:
            self.logger.error(f"[{self.pipeline_name}] Failed to upload MCAP: {exc}")
        finally:
            self._is_uploading = False

    async def _upload_to_reductstore(self, file_index: int):
        """Upload the MCAP file to ReductStore."""
        content_length = self._buffer.tell()
        self._buffer.seek(0)
        labels: dict[str, str] = dict(self.config.static_labels)

        if self.label_tracker is not None:
            labels.update(self.label_tracker.get_labels())

        await self.bucket.write(
            entry_name=self.pipeline_name,
            data=self._read_in_chunks(),
            timestamp=file_index,
            content_length=content_length,
            content_type="application/mcap",
            labels=labels,
        )

    async def _read_in_chunks(
        self, chunk_size: int = 100_000
    ) -> AsyncGenerator[bytes, None]:
        """Yield chunks from the buffer."""
        while True:
            chunk = self._buffer.read(chunk_size)
            if not chunk:
                break
            yield chunk

    async def flush_and_upload_batch(self):
        """Compatibility method for unified interface - triggers finish_and_upload."""
        # Trigger upload of current MCAP
        await self.finish_and_upload()

        # Save current schema mappings before reinitializing
        saved_schemas_by_type = self.schema_by_type.copy()
        saved_schemas_by_topic = self.schemas_by_topic.copy()

        # Reset the writer after upload
        self._init_writer()

        # Register schemas in the new writer
        for msg_type_str in saved_schemas_by_type.keys():
            source = LocalMessageDefinitionSource()
            msg_def = source.get_full_text(msg_type_str)
            new_schema = self.writer.register_msgdef(
                datatype=msg_def.topic_type,
                msgdef_text=msg_def.encoded_message_definition,
            )
            self.schema_by_type[msg_type_str] = new_schema

        # Restore topic-to-schema mappings
        for topic_name in saved_schemas_by_topic.keys():
            for msg_type_str, schema in saved_schemas_by_type.items():
                if schema == saved_schemas_by_topic[topic_name]:
                    self.schemas_by_topic[topic_name] = self.schema_by_type[
                        msg_type_str
                    ]
                    break

    def get_schema_for_topic(self, topic_name: str) -> Schema | None:
        """Get the registered schema for a topic."""
        return self.schemas_by_topic.get(topic_name)

    def has_schema_for_topic(self, topic_name: str) -> bool:
        """Check if a schema is registered for a topic."""
        return topic_name in self.schemas_by_topic

    @property
    def size(self) -> int:
        """Get current buffer size."""
        return self.current_size
