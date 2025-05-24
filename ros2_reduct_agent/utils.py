"""
Python re-implementation of the C++ message-definition loader that ships with
rosbag2 (ROS 2) at /rosbag2_storage_mcap/rosbag2_storage_mcap/src/message_definition_cache.cpp
Original source (Apache 2.0):
"""

import os

from ament_index_python.packages import get_package_share_directory
from rosidl_adapter.parser import parse_message_file


def normalize_msg_type(type_str: str) -> str:
    """
    Normalize a field type string to the 'pkg/msg/Type' format,
    stripping array suffixes like '[]' if present.
    """
    if type_str.endswith("[]"):
        type_str = type_str[:-2]
    if "/" in type_str and "/msg/" not in type_str:
        pkg, msg = type_str.split("/")
        return f"{pkg}/msg/{msg}"
    return type_str


def get_message_schema(msg_type_str: str, visited: set[str] = None) -> str:
    """
    Generate a ROS 2 .msg schema definition in the ros2msg MCAP format.
    Includes dependencies as delimited MSG blocks.
    """
    if visited is None:
        visited = set()

    msg_type_str = normalize_msg_type(msg_type_str)

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
        type_str = normalize_msg_type(str(field.type))
        if "/msg/" in type_str:
            result.append(get_message_schema(type_str, visited))

    return "\n".join(result)
