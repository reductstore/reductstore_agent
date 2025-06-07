#!/usr/bin/env python3

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

"""Launch file for the reductstore_agent node."""


import os

from ament_index_python import get_package_share_directory
from launch_ros.actions import Node, SetParameter

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration


def generate_launch_description():
    """Generate the launch description for the reductstore_agent node."""
    remappable_topics = [
        DeclareLaunchArgument("input_topic", default_value="~/input"),
    ]

    args = [
        DeclareLaunchArgument(
            "name", default_value="reductstore_agent", description="node name"
        ),
        DeclareLaunchArgument(
            "namespace", default_value="", description="node namespace"
        ),
        DeclareLaunchArgument(
            "params",
            default_value=os.path.join(
                get_package_share_directory("reductstore_agent"),
                "config",
                "params.yml",
            ),
            description="path to parameter file",
        ),
        DeclareLaunchArgument(
            "log_level",
            default_value="info",
            description="ROS logging level (debug, info, warn, error, fatal)",
        ),
        DeclareLaunchArgument(
            "use_sim_time", default_value="false", description="use simulation clock"
        ),
        *remappable_topics,
    ]

    nodes = [
        Node(
            package="reductstore_agent",
            executable="recorder",
            namespace=LaunchConfiguration("namespace"),
            name=LaunchConfiguration("name"),
            parameters=[LaunchConfiguration("params")],
            arguments=[
                "--ros-args",
                "--log-level",
                LaunchConfiguration("log_level"),
            ],
            remappings=[
                (la.default_value[0].text, LaunchConfiguration(la.name))
                for la in remappable_topics
            ],
            output="screen",
            emulate_tty=True,
        )
    ]

    return LaunchDescription(
        [
            *args,
            SetParameter("use_sim_time", LaunchConfiguration("use_sim_time")),
            *nodes,
        ]
    )
