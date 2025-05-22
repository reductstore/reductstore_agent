import os

from ament_index_python.packages import get_package_share_directory
from rosidl_adapter.parser import parse_message_file


def get_message_schema(msg_type_str: str, visited: set[str] = None) -> str:
    """
    Generate a ROS2 .msg schema definition in the ros2msg MCAP format.
    Includes dependencies as delimited MSG blocks.
    """
    if visited is None:
        visited = set()

    if msg_type_str in visited:
        return ""

    visited.add(msg_type_str)

    try:
        pkg, msg = msg_type_str.split("/msg/")
        msg_path = os.path.join(get_package_share_directory(pkg), "msg", f"{msg}.msg")
        parsed = parse_message_file(pkg, msg_path)
    except Exception as e:
        return f"# Failed to parse {msg_type_str}: {e}"

    result = []
    if len(visited) == 1:
        result.append(str(parsed))
    else:
        result.append("=" * 80)
        result.append(f"MSG: {pkg}/msg/{msg}")
        result.append(str(parsed))

    for field in parsed.fields:
        type_str = str(field.type)
        if "/" in type_str:
            if "/msg/" not in type_str:
                pkg, msg = type_str.split("/")
                type_str = f"{pkg}/msg/{msg}"
            result.append(get_message_schema(type_str, visited))

    return "\n".join(result)
