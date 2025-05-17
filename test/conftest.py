import pytest
import rclpy


@pytest.fixture(scope="session", autouse=True)
def ros_context():
    """Initialize rclpy for the test session."""
    rclpy.init()
    yield
    rclpy.shutdown()
