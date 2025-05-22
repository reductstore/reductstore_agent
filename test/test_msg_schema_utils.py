from ros2_reduct_agent.utils import get_message_schema


def test_simple_builtin_type():
    schema = get_message_schema("std_msgs/msg/String")
    assert "string data" in schema
    assert "MSG:" not in schema


def test_nested_type():
    schema = get_message_schema("sensor_msgs/msg/Image")
    assert "std_msgs/Header header" in schema
    assert "MSG: std_msgs/msg/Header" in schema
    assert "MSG: builtin_interfaces/msg/Time" in schema


def test_pose_stamped():
    schema = get_message_schema("geometry_msgs/msg/PoseStamped")
    assert "std_msgs/Header header" in schema
    assert "geometry_msgs/Pose pose" in schema
    assert "MSG: geometry_msgs/msg/Pose" in schema
    assert "MSG: geometry_msgs/msg/Point" in schema
    assert "MSG: geometry_msgs/msg/Quaternion" in schema


def test_invalid_type():
    schema = get_message_schema("nonexistent_pkg/msg/FakeType")
    assert "# Failed to parse" in schema
