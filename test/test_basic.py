import rclpy
from ros2_reduct_agent.recorder import Recorder

def test_node_lifecycle():
    rclpy.init()
    node = rclpy.create_node('test_node')
    assert node.get_name() == 'test_node'
    node.destroy_node()
    rclpy.shutdown()

def test_recorder_lifecycle():
    rclpy.init()
    try:
        from rclpy.parameter import Parameter
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
