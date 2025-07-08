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

"""Configuration models for the reductstore_agent package."""

from enum import Enum
from tempfile import SpooledTemporaryFile

from mcap.records import Schema
from mcap_ros2.writer import Writer as McapWriter
from pydantic import BaseModel, ConfigDict, Field, field_validator
from rclpy.timer import Timer
from reduct import QuotaType

from .utils import parse_bytes_with_si_units


class StorageConfig(BaseModel):
    """Configuration for ReductStore storage connection and bucket settings."""

    url: str
    bucket: str
    api_token: str = ""
    quota_type: QuotaType = QuotaType.NONE
    quota_size: int | None = None
    max_block_size: int | None = None
    max_block_records: int | None = None

    @field_validator("url", "bucket")
    @classmethod
    def not_empty(cls, v, info):
        """Ensure string fields are not empty."""
        if not v.strip():
            raise ValueError(f"'{info.field_name}' must not be empty")
        return v

    @field_validator("quota_type", mode="before")
    @classmethod
    def validate_quota_type(cls, v):
        """Manually validate quota_type since reduct.QuotaType is not a str Enum."""
        if isinstance(v, QuotaType):
            return v
        try:
            return QuotaType[v.upper()]
        except (KeyError, AttributeError):
            raise ValueError(f"Invalid quota type: '{v}'")

    @field_validator("quota_size", "max_block_size", mode="before")
    @classmethod
    def parse_si_units(cls, value):
        """Parse SI units for size values."""
        if value is None:
            return None
        return parse_bytes_with_si_units(value)


class FilenameMode(str, Enum):
    """Filename mode for pipeline segments."""

    TIMESTAMP = "timestamp"
    INCREMENTAL = "incremental"


class PipelineConfig(BaseModel):
    """Configuration for a recording pipeline."""

    split_max_duration_s: int = Field(..., alias="split.max_duration_s", ge=1, le=3600)
    split_max_size_bytes: int | None = Field(
        None, alias="split.max_size_bytes", ge=1_000, le=1_000_000_000
    )
    chunk_size_bytes: int = Field(
        1_000_000,
        alias="chunk_size_bytes",
        ge=1_000,
        le=10_000_000,
    )
    compression: str = Field(
        "zstd",
        alias="compression",
        pattern=r"^(none|lz4|zstd)$",
    )
    enable_crcs: bool = Field(True, alias="enable_crcs")
    spool_max_size_bytes: int = Field(
        10_000_000,
        alias="spool_max_size_bytes",
        ge=1_000,
        le=1_000_000_000,
    )
    include_topics: list[str] = Field(..., alias="include_topics")
    filename_mode: FilenameMode = Field(FilenameMode.TIMESTAMP, alias="filename_mode")

    @field_validator("include_topics")
    @classmethod
    def topics_must_be_ros_names(cls, value):
        """Ensure topics are strings starting with '/'."""
        if not isinstance(value, list) or not all(
            isinstance(t, str) and t.startswith("/") for t in value
        ):
            raise ValueError(
                "'include_topics' must be a list of ROS topic names starting with '/'"
            )
        return value

    @field_validator(
        "split_max_size_bytes",
        "spool_max_size_bytes",
        "chunk_size_bytes",
        "spool_max_size_bytes",
        mode="before",
    )
    @classmethod
    def parse_si_units(cls, value):
        """Parse SI units for byte values."""
        return parse_bytes_with_si_units(value)

    def format_for_log(self) -> str:
        """Format the pipeline config for logging."""
        config_items = self.model_dump(by_alias=True)
        max_key_len = max(len(k) for k in config_items)
        indent = " " * 8
        lines = []

        for key in sorted(config_items):
            value = config_items[key]
            if isinstance(value, list):
                value_str = "[" + ", ".join(repr(i) for i in value) + "]"
            elif isinstance(value, FilenameMode):
                value_str = value.value
            elif isinstance(value, str):
                value_str = f'"{value}"'
            else:
                value_str = str(value)

            lines.append(f"{indent}{key.ljust(max_key_len)} = {value_str}")
        return "\n".join(lines)


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
