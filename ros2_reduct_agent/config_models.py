from enum import Enum
from io import BytesIO

from mcap.writer import Writer as McapWriter
from pydantic import BaseModel, Field, field_validator
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
    COUNTER = "counter"


class PipelineConfig(BaseModel):
    split_max_duration_s: int = Field(..., alias="split.max_duration_s", ge=1, le=3600)
    split_max_size_bytes: int | None = Field(
        None, alias="split.max_size_bytes", ge=1000, le=1_000_000_000
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
    channels: dict[str, int] = Field(default_factory=dict)
    schemas: dict[str, int] = Field(default_factory=dict)
    topics: list[str] = Field(default_factory=list)
    counter: int = 0
    timestamp: int | None = None
    buffer: BytesIO | None = None
    writer: McapWriter | None = None
    timer: Timer | None = None
    current_size: int = 0
    is_uploading: bool = False

    class Config:
        arbitrary_types_allowed = True
