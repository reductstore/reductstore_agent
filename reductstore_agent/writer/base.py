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

"""Base interface for output writers."""

from abc import ABC, abstractmethod
from typing import Any


class OutputWriter(ABC):
    """Abstract base class for output writers providing unified interface."""

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema for a topic and message type."""
        pass

    @abstractmethod
    def write_message(self, message: Any, publish_time: int, topic: str, **kwargs):
        """Write message to output."""

    @abstractmethod
    async def flush_and_upload_batch(self):
        """Flush and upload any pending data."""

    @property
    @abstractmethod
    def size(self) -> int:
        """Get current buffer/batch size."""
