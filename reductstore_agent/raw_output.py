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


from rclpy.serialization import serialize_message
from reductstore_agent.config_models import PipelineConfig
from typing import Any
import io
from tempfile import SpooledTemporaryFile
import json
from .utils import ns_to_us, ros2_type_name
import struct


BATCH_LIMIT = 100 * 1024
FRAME_HEADER = struct.Struct("<QII")


class RawOutputWriter:
    def __init__(
            self,
            bucket, 
            buffer: SpooledTemporaryFile[bytes],
            pipeline_name: PipelineConfig,
            # spool_max_size: int
    ):
        self.bucket = bucket
        self.buffer = buffer
        # self.spool_max_size = spool_max_size
        self.pipeline_name = pipeline_name


    async def upload_to_reductstore(self, pipeline_name, serialized_data: bytes, timestamp: int, labels):
        await self.client.bucket.write(
            pipeline_name,
            timestamp, # use as index
            serialized_data,
            labels
        )


    def write_message(self, message: Any, publish_time: int):
        try: 
            serialized_data = serialize_message(message)
        except:
            return
        
        # 2. Add labels timestamp etc
        timestamp_ms =  ns_to_us(publish_time)
        topic_type = ros2_type_name(message)
        labels = {
            "type": f"{topic_type} Message",
            "serialization": "cdr",
        }
        record_size = len(serialized_data)
        if record_size >= self.BATCH_LIMIT:
            # Upload because streaming
            # Flush and upload buffer first and then stream this
            self.flush_buffer(self.buffer)
            # Do we combine the data first ?
            # Because if we do it like this, two calls are made
            self.upload_to_reductstore(self.pipeline_name, serialized_data, timestamp_ms, labels)
        
        # If smaller than 100KB batch the record into buffer
        self.append_record(self.buffer, timestamp_ms, labels, serialized_data)


def append_record(self, buffer: SpooledTemporaryFile, ts_us: int, labels: dict, serialized_data: bytes) -> None:
    labels_bytes = json.dumps(labels, separators=(",", ":")).encode("utf-8")
    # Have to add a header for the buffer, to later extract data out of it
    hdr = self.FRAME_HEADER.pack(ts_us, len(labels_bytes), len(serialized_data))

    buffer.seek(0, io.SEEK_END)
    buffer.write(hdr)
    buffer.write(labels_bytes)
    buffer.write(serialized_data)


def flush_buffer(self, buffer: SpooledTemporaryFile):
    buffer.seek(0)
    read = buffer.read
    while True:
        hdr = read(self.FRAME_HEADER.size)
        if not hdr:
            break
        timestamp_ms, l_len, p_len = self.FRAME_HEADER.unpack(hdr)
        labels = json.loads(read(l_len).decode("utf-8"))
        serialized_data = read(p_len)

        self.upload_to_reductstore(self.pipeline_name, serialized_data, timestamp_ms, labels)

    buffer.seek(0)
    buffer.truncate(0)
    

    



