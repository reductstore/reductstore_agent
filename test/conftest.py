from typing import Generator

import pytest
import rclpy
from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.publisher import Publisher
from reduct import Client
from std_msgs.msg import String

from ros2_reduct_agent.recorder import Recorder
from ros2_reduct_agent.utils import get_or_create_event_loop


@pytest.fixture
def reduct_client():
    """Provides a clean ReductStore client by recreating the test bucket before and after the session."""
    loop = get_or_create_event_loop()
    client = Client("http://localhost:8383", api_token="test_token")

    async def cleanup():
        bucket_list = await client.list()
        if "test_bucket" in [bucket.name for bucket in bucket_list]:
            bucket = await client.get_bucket("test_bucket")
            await bucket.remove()

    loop.run_until_complete(cleanup())

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
    node = Node("test_publisher")
    yield node
    node.destroy_node()


@pytest.fixture
def publisher(publisher_node: Node) -> Publisher:
    pub = publisher_node.create_publisher(String, "/test/topic", 10)
    return pub


@pytest.fixture
def basic_recorder() -> Generator[Recorder, None, None]:
    """Recorder configured to slice every 1s and upload /test/topic."""
    params = [
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
def low_chunk_recorder() -> Generator[Recorder, None, None]:
    """Recorder configured with low chunk size and no compression for large message test."""
    params = [
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
    """Recorder with two parallel pipelines: /test/topic and /rosout"""
    params = [
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
