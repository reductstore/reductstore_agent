import rclpy
from ros2_reduct_agent.recorder import Recorder
from rclpy.parameter import Parameter
import pytest

# Test that a basic node can be created and destroyed
def test_node_lifecycle():
    rclpy.init()
    node = rclpy.create_node('test_node')
    assert node.get_name() == 'test_node'
    node.destroy_node()
    rclpy.shutdown()

# Test that Recorder node can be created and destroyed with required parameters
def test_recorder_lifecycle():
    rclpy.init()
    try:
        param_overrides = [
            Parameter('storage.url', Parameter.Type.STRING, 'http://localhost:8383'),
            Parameter('storage.api_token', Parameter.Type.STRING, 'dummy_token'),
            Parameter('storage.bucket', Parameter.Type.STRING, 'test_bucket'),
            Parameter('pipelines.telemetry.include_topics', Parameter.Type.STRING_ARRAY, ['/recorder/input']),
            Parameter('pipelines.telemetry.split.max_duration_s', Parameter.Type.INTEGER, 5),
            Parameter('pipelines.telemetry.split.max_size_bytes', Parameter.Type.INTEGER, 100),
        ]
        node = Recorder(parameter_overrides=param_overrides)
        assert node.get_name() == 'recorder'
        node.destroy_node()
    finally:
        rclpy.shutdown()

# Test that Recorder fails gracefully if a required parameter is missing
def test_recorder_missing_param():
    rclpy.init()
    try:
        # Only provide some parameters, omit storage.url
        param_overrides = [
            Parameter('storage.api_token', Parameter.Type.STRING, 'dummy_token'),
            Parameter('storage.bucket', Parameter.Type.STRING, 'test_bucket'),
            Parameter('pipelines.telemetry.include_topics', Parameter.Type.STRING_ARRAY, ['/recorder/input']),
            Parameter('pipelines.telemetry.split.max_duration_s', Parameter.Type.INTEGER, 5),
            Parameter('pipelines.telemetry.split.max_size_bytes', Parameter.Type.INTEGER, 100),
        ]
        with pytest.raises(SystemExit):
            Recorder(parameter_overrides=param_overrides)
    finally:
        rclpy.shutdown()

# Test that Recorder can parse pipeline config with multiple topics
def test_recorder_multiple_topics():
    rclpy.init()
    try:
        param_overrides = [
            Parameter('storage.url', Parameter.Type.STRING, 'http://localhost:8383'),
            Parameter('storage.api_token', Parameter.Type.STRING, 'dummy_token'),
            Parameter('storage.bucket', Parameter.Type.STRING, 'test_bucket'),
            Parameter('pipelines.telemetry.include_topics', Parameter.Type.STRING_ARRAY, ['/recorder/input', '/recorder/other']),
            Parameter('pipelines.telemetry.split.max_duration_s', Parameter.Type.INTEGER, 5),
            Parameter('pipelines.telemetry.split.max_size_bytes', Parameter.Type.INTEGER, 100),
        ]
        node = Recorder(parameter_overrides=param_overrides)
        assert '/recorder/input' in node.mcap_pipelines['telemetry']['topics']
        assert '/recorder/other' in node.mcap_pipelines['telemetry']['topics']
        node.destroy_node()
    finally:
        rclpy.shutdown()
