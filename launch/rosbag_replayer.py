#!/usr/bin/env python3

# Copyright 2026 ReductSoftware UG
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

"""Launch file for rosbag_replayer node."""

from launch_ros.actions import Node, SetParameter

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    """Generate the launch description for the rosbag_replayer node."""
    demo_bag_path = "demo_bags/test.mcap"

    remappable_topics = [
        DeclareLaunchArgument("bag_path", default_value=demo_bag_path),
    ]

    args = [
        DeclareLaunchArgument(
            "name", default_value="rosbag_replayer", description="node name"
        ),
        DeclareLaunchArgument(
            "namespace", default_value="", description="node namespace"
        ),
        DeclareLaunchArgument(
            "params",
            default_value=demo_bag_path,
            description="path to rosbag file",
        ),
        DeclareLaunchArgument(
            "log_level",
            default_value="info",
            description="log level for the rosbag_replayer node",
        ),
    ]

    rosbag_replayer_node = Node(
        package="reductstore_agent",
        executable="rosbag_replayer",
        name=LaunchConfiguration("name"),
        namespace=LaunchConfiguration("namespace"),
        parameters=[
            SetParameter(name="log_level", value=LaunchConfiguration("log_level")),
            {
                "bag_path": LaunchConfiguration("bag_path"),
            },
        ],
        output="screen",
    )

    return LaunchDescription(args + remappable_topics + [rosbag_replayer_node])
