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

"""Define pipeline state."""


from tempfile import SpooledTemporaryFile

from mcap.records import Schema
from mcap_ros2.writer import Writer as McapWriter
from pydantic import BaseModel, ConfigDict, Field
from rclpy.timer import Timer

from .downsampler import Downsampler


class PipelineState(BaseModel):
    """State for a recording pipeline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    topics: list[str] = Field(default_factory=list)
    schemas_by_topic: dict[str, Schema] = Field(default_factory=dict)
    schema_by_type: dict[str, Schema] = Field(default_factory=dict)
    increment: int = 0
    first_timestamp: int | None = None
    buffer: SpooledTemporaryFile[bytes] | None = None
    writer: McapWriter | None = None
    timer: Timer | None = None
    current_size: int = 0
    is_uploading: bool = False
    downsampler: Downsampler | None = None
