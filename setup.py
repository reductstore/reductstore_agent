"""ROS2 Reduct Agent Setup Script."""

import os
from glob import glob

from setuptools import find_packages, setup

packages = find_packages(include=["reductstore_agent", "reductstore_agent.*"])
console_scripts = ["recorder = reductstore_agent.recorder:main"]

if os.path.isdir("rosbag_replayer"):
    # include rosbag_replayer and any subpackages too
    packages += find_packages(include=["rosbag_replayer", "rosbag_replayer.*"])
    console_scripts.append("rosbag_replayer = rosbag_replayer.rosbag_replayer:main")

setup(
    name="reductstore_agent",
    version="0.2.0",
    packages=packages,
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/reductstore_agent"]),
        (os.path.join("share", "reductstore_agent"), ["package.xml"]),
        (
            os.path.join("share", "reductstore_agent", "launch"),
            glob("launch/*launch.[pxy][yma]*"),
        ),
        (os.path.join("share", "reductstore_agent", "config"), glob("config/*")),
    ],
    install_requires=[
        "setuptools",
        "reduct-py",
        "mcap",
        "mcap-ros2-support",
        "PyYAML",
        "lark",
        "numpy",
    ],
    extras_require={
        "test": ["pytest", "flake8", "pydocstyle"],
    },
    zip_safe=True,
    maintainer="Anthony",
    maintainer_email="info@reduct.store",
    description="ROS2 Reduct Agent",
    license="MIT",
    entry_points={"console_scripts": console_scripts},
)
