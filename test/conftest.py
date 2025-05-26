import asyncio

import pytest
import rclpy
from reduct import Client

REDUCT_URL = "http://localhost:8383"
REDUCT_API_TOKEN = "test_token"
TEST_BUCKET = "test_bucket"


@pytest.fixture(scope="session", autouse=True)
def ros_context():
    """Initialize rclpy for the test session."""
    rclpy.init()
    yield
    rclpy.shutdown()


@pytest.fixture(scope="function")
def reduct_client():
    """Provides a clean ReductStore client by recreating the test bucket before and after the session."""
    loop = asyncio.get_event_loop()
    client = Client(REDUCT_URL, api_token=REDUCT_API_TOKEN)

    async def cleanup():
        bucket_list = await client.list()
        if TEST_BUCKET in [bucket.name for bucket in bucket_list]:
            bucket = await client.get_bucket(TEST_BUCKET)
            await bucket.remove()

    loop.run_until_complete(cleanup())

    yield client

    loop.run_until_complete(cleanup())
