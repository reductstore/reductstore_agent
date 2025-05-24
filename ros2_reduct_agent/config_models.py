from enum import Enum
from tempfile import SpooledTemporaryFile

from mcap.records import Schema
from mcap_ros2.writer import Writer as McapWriter
from pydantic import BaseModel, ConfigDict, Field, field_validator
from rclpy.timer import Timer


class StorageConfig(BaseModel):
    url: str
    api_token: str
    bucket: str

    @field_validator("url", "api_token", "bucket")
    @classmethod
    def not_empty(cls, v, info):
        if not v:
            raise ValueError(f"'{info.field_name}' must not be empty")
        return v


class FilenameMode(str, Enum):
    """Filename mode for pipeline segments."""

    TIMESTAMP = "timestamp"
    INCREMENTAL = "incremental"


class PipelineConfig(BaseModel):
    split_max_duration_s: int = Field(..., alias="split.max_duration_s", ge=1, le=3600)
    split_max_size_bytes: int | None = Field(
        None, alias="split.max_size_bytes", ge=1000, le=1_000_000_000
    )
    spool_max_size_bytes: int = Field(
        10 * 1024 * 1024,
        alias="spool_max_size_bytes",
        ge=1,
        le=1_000_000_000,
    )
    include_topics: list[str] = Field(..., alias="include_topics")
    filename_mode: FilenameMode = Field(FilenameMode.TIMESTAMP, alias="filename_mode")

    @field_validator("include_topics")
    @classmethod
    def topics_must_be_ros_names(cls, v, info):
        if not isinstance(v, list) or not all(
            isinstance(t, str) and t.startswith("/") for t in v
        ):
            raise ValueError(
                "'include_topics' must be a list of ROS topic names starting with '/'"
            )
        return v


class PipelineState(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    topics: list[str] = Field(default_factory=list)
    schemas: dict[str, Schema] = Field(default_factory=dict)
    increment: int = 0
    first_time: int | None = None
    buffer: SpooledTemporaryFile[bytes] | None = None
    writer: McapWriter | None = None
    timer: Timer | None = None
    current_size: int = 0
    is_uploading: bool = False
