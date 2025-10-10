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

import re
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
        ge=1_000,
        le=10_000_000,
    )
    compression: str = Field(
        "zstd",
        pattern=r"^(none|lz4|zstd)$",
    )
    enable_crcs: bool = True
    spool_max_size_bytes: int = Field(
        10_000_000,
        ge=1_000,
        le=1_000_000_000,
    )

    include_topics: list[str] = Field(default_factory=list)
    exclude_topics: list[str] = Field(default_factory=list)
    static_labels: dict[str, str] = Field(default_factory=dict)
    filename_mode: FilenameMode = FilenameMode.TIMESTAMP

    downsampling_mode: str = Field(
        "none",
        pattern=r"^(none|max_rate|stride)$"
    )
    max_rate_hz: float | None = Field(
        None, ge=0.0
    )
    stride_n: int | None = Field(
        None, ge=2
    )

    @field_validator("include_topics", "exclude_topics")
    @classmethod
    def validate_topics_list(cls, value):
        """Ensure provided strings are valid regex patterns."""
        if not isinstance(value, list):
            raise ValueError("Value must be a list of regex patterns")
        for pattern in value:
            if not isinstance(pattern, str):
                raise ValueError("Regex patterns must be strings")
            try:
                re.compile(pattern)
            except re.error as exc:
                raise ValueError(f"Invalid regex pattern '{pattern}': {exc}")
        return value

    @field_validator("static_labels")
    @classmethod
    def non_empty_labels(cls, value):
        """Validate that label keys and values are non-empty strings."""
        if not isinstance(value, dict):
            raise ValueError("'static_labels' must be a mapping")
        for k, v in value.items():
            if not isinstance(k, str) or not k.strip():
                raise ValueError("Label keys must be non-empty strings")
            if not isinstance(v, str) or not v.strip():
                raise ValueError("Label values must be non-empty strings")
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
    msg_counter: int = 0
    last_recorded_timestamp: int | None = None
