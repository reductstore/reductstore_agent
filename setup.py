"""ROS2 Reduct Agent Setup Script."""

import os
from glob import glob

from setuptools import setup

package_name = "ros2_reduct_agent"

setup(
    name=package_name,
    version="0.0.0",
    packages=[package_name],
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        (os.path.join("share", package_name), ["package.xml"]),
        (
            os.path.join("share", package_name, "launch"),
            glob("launch/*launch.[pxy][yma]*"),
        ),
        (os.path.join("share", package_name, "config"), glob("config/*")),
    ],
    install_requires=[
        "setuptools",
        "mcap",
        "mcap-ros2-support",
        "rosbag2_py",
        "reduct",
    ],
    zip_safe=True,
    maintainer="Anthony",
    maintainer_email="info@reduct.store",
    description="ROS2 Reduct Agent",
    license="MIT",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": ["recorder = ros2_reduct_agent.recorder:main"],
    },
)
