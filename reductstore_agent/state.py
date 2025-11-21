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


from mcap.records import Schema
from rclpy.timer import Timer

from .downsampler import Downsampler
from .dynamic_labels import LabelStateTracker
from .writer.base import OutputWriter


class PipelineState:
    """State for a recording pipeline."""

    def __init__(
        self,
        topics: list[str] | None = None,
        schemas_by_topic: dict[str, str] | None = None,
        schema_by_type: dict[str, Schema] | None = None,
        increment: int = 0,
        first_timestamp: int | None = None,
        writer: OutputWriter | None = None,
        timer: Timer | None = None,
        current_size: int = 0,
        is_uploading: bool = False,
        downsampler: Downsampler | None = None,
        label_mode: LabelStateTracker | None = None,
    ):
        """Initialize pipeline state."""
        self.topics = topics if topics is not None else []
        self.schemas_by_topic = schemas_by_topic if schemas_by_topic is not None else {}
        self.schema_by_type = schema_by_type if schema_by_type is not None else {}
        self.increment = increment
        self.first_timestamp = first_timestamp
        self.writer = writer
        self.timer = timer
        self.current_size = current_size
        self.is_uploading = is_uploading
        self.downsampler = downsampler
        self.label_mode = label_mode
