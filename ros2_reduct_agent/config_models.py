from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


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


class PipelineConfig(BaseModel):
    split_max_duration_s: int = Field(..., alias="split.max_duration_s", ge=1, le=3600)
    split_max_size_bytes: Optional[int] = Field(
        None, alias="split.max_size_bytes", ge=1000, le=1_000_000_000
    )
    include_topics: List[str] = Field(..., alias="include_topics")

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
