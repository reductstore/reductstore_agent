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

"""Fixtures for testing the reductstore_agent package."""

from typing import Generator

import pytest
import rclpy
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.publisher import Publisher
from reduct import Client
from std_msgs.msg import String

from reductstore_agent.models import LabelMode, LabelTopicConfig, PipelineConfig
from reductstore_agent.recorder import Recorder
from reductstore_agent.utils import get_or_create_event_loop

from .config.test_recorder_params import (
    as_overrides,
    downsampling_params_max_rate,
    downsampling_params_none,
    downsampling_params_stride,
    output_format_params_cdr,
    pipeline_params,
    storage_params,
)


@pytest.fixture
def reduct_client():
    """Recreating the test bucket before and after the session."""
    loop = get_or_create_event_loop()
    client = Client("http://localhost:8383", api_token="test_token")

    async def cleanup():
        bucket_list = await client.list()
        if "test_bucket" in [bucket.name for bucket in bucket_list]:
            bucket = await client.get_bucket("test_bucket")
            await bucket.remove()

    yield client

    loop.run_until_complete(cleanup())


@pytest.fixture(scope="session", autouse=True)
def ros_context():
    """Initialize rclpy for the test session."""
    rclpy.init()
    yield
    rclpy.shutdown()


@pytest.fixture
def publisher_node() -> Generator[Node, None, None]:
    """Create a publisher node for testing."""
    node = Node("test_publisher")
    yield node
    node.destroy_node()


@pytest.fixture
def publisher(publisher_node: Node) -> Publisher:
    """Create a publisher for the test topic."""
    pub = publisher_node.create_publisher(String, "/test/topic", 50)
    return pub


@pytest.fixture
def basic_recorder() -> Generator[Recorder, None, None]:
    """Record every 1s and upload MCAP file with /test/topic."""
    params = [
        Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0),
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        Parameter(
            "pipelines.timer_test_topic.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.timer_test_topic.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.timer_test_topic.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
    ]
    rec = Recorder(parameter_overrides=params)
    yield rec
    rec.destroy_node()


@pytest.fixture
def quota_recorder() -> Generator[Recorder, None, None]:
    """Record with quota settings."""
    params = [
        Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0),
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        Parameter(
            "storage.quota_type",
            Parameter.Type.STRING,
            "FIFO",
        ),
        Parameter(
            "storage.quota_size",
            Parameter.Type.INTEGER,
            100_000_000,
        ),
        Parameter(
            "storage.max_block_size",
            Parameter.Type.INTEGER,
            10_000_000,
        ),
        Parameter(
            "storage.max_block_records",
            Parameter.Type.INTEGER,
            2048,
        ),
    ]
    rec = Recorder(parameter_overrides=params)
    yield rec
    rec.destroy_node()


@pytest.fixture
def low_chunk_recorder() -> Generator[Recorder, None, None]:
    """Record with low chunk size and no compression for large message test."""
    params = [
        Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0),
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        Parameter(
            "pipelines.timer_test_topic.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.timer_test_topic.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.timer_test_topic.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
        Parameter(
            "pipelines.timer_test_topic.chunk_size_bytes",
            Parameter.Type.INTEGER,
            1024,
        ),
        Parameter(
            "pipelines.timer_test_topic.compression",
            Parameter.Type.STRING,
            "none",
        ),
    ]
    rec = Recorder(parameter_overrides=params)
    yield rec
    rec.destroy_node()


@pytest.fixture
def parallel_recorder() -> Generator[Recorder, None, None]:
    """Record with two parallel pipelines: /test/topic and /rosout."""
    params = [
        Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0),
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        # pipeline A: /test/topic
        Parameter(
            "pipelines.timer_test_topic.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.timer_test_topic.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.timer_test_topic.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
        # pipeline B: /rosout
        Parameter(
            "pipelines.timer_rosout.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/rosout"],
        ),
        Parameter(
            "pipelines.timer_rosout.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.timer_rosout.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
    ]
    rec = Recorder(parameter_overrides=params)
    yield rec
    rec.destroy_node()


@pytest.fixture
def labels_recorder() -> Generator[Recorder, None, None]:
    """Record with static labels and two pipelines: labeled and unlabeled."""
    params = [
        Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0),
        Parameter("storage.url", Parameter.Type.STRING, "http://localhost:8383"),
        Parameter("storage.api_token", Parameter.Type.STRING, "test_token"),
        Parameter("storage.bucket", Parameter.Type.STRING, "test_bucket"),
        Parameter(
            "pipelines.labeled.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.labeled.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.labeled.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
        Parameter(
            "pipelines.labeled.static_labels.source",
            Parameter.Type.STRING,
            "telemetry",
        ),
        Parameter(
            "pipelines.labeled.static_labels.robot",
            Parameter.Type.STRING,
            "alpha",
        ),
        # Second pipeline without labels
        Parameter(
            "pipelines.unlabeled.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.unlabeled.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.unlabeled.filename_mode",
            Parameter.Type.STRING,
            "incremental",
        ),
    ]
    rec = Recorder(parameter_overrides=params)
    yield rec
    rec.destroy_node()


@pytest.fixture
def cdr_output_recorder() -> Generator[Recorder, None, None]:
    """Init a cdr_output Recorder node."""
    additional_params = [Parameter("subscription_delay_s", Parameter.Type.DOUBLE, 0.0)]

    all_overrides = (
        as_overrides(
            storage_params(),
            pipeline_params(),
            downsampling_params_none(),
            output_format_params_cdr(),
        )
        + additional_params
    )

    rec = Recorder(parameter_overrides=all_overrides)
    try:
        yield rec
    finally:
        # Only clean up if shutdown hasn't already been called in the test
        if rclpy.ok():
            rec.destroy_node()


@pytest.fixture
def stride_recorder() -> Generator[Recorder, None, None]:
    """Init a stride recorder node."""
    all_overrides = as_overrides(
        storage_params(), pipeline_params(), downsampling_params_stride()
    )

    rec = Recorder(parameter_overrides=all_overrides)
    yield rec
    rec.destroy_node()


@pytest.fixture
def max_rate_recorder() -> Generator[Recorder, None, None]:
    """Init a max_rate recorder node."""
    all_overrides = as_overrides(
        storage_params(), pipeline_params(), downsampling_params_max_rate()
    )

    rec = Recorder(parameter_overrides=all_overrides)
    yield rec
    rec.destroy_node()


@pytest.fixture
def mock_label_config():
    """Return a mock_label PipelineConfig."""
    required_split_params = {
        "split.max_duration_s": 1,
    }

    return PipelineConfig(
        labels=[
            LabelTopicConfig(
                topic="/mission_info",
                mode=LabelMode.LAST,
                fields={"mission_id": "mid"},
            ),
            LabelTopicConfig(
                topic="/telemetry",
                mode=LabelMode.MAX,
                fields={"max_speed": "speed"},
            ),
            LabelTopicConfig(
                topic="/startup_data",
                mode=LabelMode.FIRST,
                fields={"initial_voltage": "volt"},
            ),
        ],
        **required_split_params,
    )
