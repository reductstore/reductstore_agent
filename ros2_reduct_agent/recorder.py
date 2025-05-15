import importlib
from typing import Any
from collections import defaultdict

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile

class Recorder(Node):

    def __init__(self):
        """Constructor"""
        super().__init__("recorder", allow_undeclared_parameters=True, 
                         automatically_declare_parameters_from_overrides=True)

        self.subscriber = None

        self.storage = self.load_and_validate_storage_config()
        self.pipelines = self.parse_and_validate_pipeline_config()

        self.setup()

    def load_and_validate_storage_config(self) -> dict[str, Any]:
            """Load and validate required storage parameters from the parameter server."""
            required_keys = ["url", "api_token", "bucket"]
            config = {}

            for key in required_keys:
                param = f"storage.{key}"
                if not self.has_parameter(param):
                    self.get_logger().error(f"[storage] Missing parameter: '{param}'")
                    raise SystemExit(1)

                value = self.get_parameter(param).value
                if not value:
                    self.get_logger().error(f"[storage] Empty value for parameter: '{param}'")
                    raise SystemExit(1)

                config[key] = value
                self.get_logger().info(f"[storage] Loaded '{param}': {value}")

            return config

    def parse_and_validate_pipeline_config(self) -> dict[str, dict[str, Any]]:
        """Parse all pipelines.* parameters and validate known fields."""
        pipelines = defaultdict(dict)

        for param in self._parameters.values():
            name = param.name
            value = param.value

            if not name.startswith("pipelines."):
                continue

            parts = name.split(".")
            if len(parts) < 3:
                self.get_logger().warn(f"[pipelines] Invalid parameter: '{name}'")
                continue

            pipeline_name = parts[1]
            subkey = ".".join(parts[2:])
            pipelines[pipeline_name][subkey] = value

            self.validate_pipeline_parameter(name, value)

        self.get_logger().info(f"[pipelines] Loaded pipeline configs: {list(pipelines.keys())}")
        return pipelines

    def validate_pipeline_parameter(self, name: str, value: Any):
        """Validate individual known pipeline parameter."""
        if name.endswith("max_duration_s"):
            if not isinstance(value, int) or not (1 <= value <= 3600):
                self.get_logger().warn(f"[pipelines] '{name}' should be an int between 1 and 3600. Got: {value}")

        elif name.endswith("max_size_bytes"):
            if not isinstance(value, int) or not (1_000_000 <= value <= 1_000_000_000):
                self.get_logger().warn(f"[pipelines] '{name}' should be an int between 1MB and 1GB. Got: {value}")

        elif name.endswith("include_topics"):
            if not isinstance(value, list) or not all(isinstance(t, str) for t in value):
                self.get_logger().warn(f"[pipelines] '{name}' should be a list of strings. Got: {value}")

    def setup(self):
        """Subscribe only to topics listed in pipelines' include_topics, with correct types."""
        self.subscribers = []
        ignore_topics = {"/rosout", "/parameter_events"}  # Add more if needed

        # Gather all unique (topic_name, type) pairs from all pipelines
        topics_to_subscribe = set()
        for pipeline in self.pipelines.values():
            include_topics = pipeline.get("include_topics", [])
            # Support both list of strings and list of dicts (for future extensibility)
            for t in include_topics:
                if isinstance(t, dict):
                    topic_name = t.get("name")
                    msg_type_str = t.get("type")
                else:
                    topic_name = t
                    # Try to find type from ROS graph below
                    msg_type_str = None
                if topic_name and topic_name not in ignore_topics:
                    topics_to_subscribe.add((topic_name, msg_type_str))

        # Get all topic types from ROS graph
        topic_types = dict(self.get_topic_names_and_types())

        for topic_name, msg_type_str in topics_to_subscribe:
            # If type is not specified in config, get it from ROS graph
            if not msg_type_str:
                types = topic_types.get(topic_name)
                if not types:
                    self.get_logger().warn(f"No type found for topic '{topic_name}'")
                    continue
                msg_type_str = types[0]
            pkg, _, msg = msg_type_str.partition('/msg/')
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError):
                self.get_logger().warn(f"Cannot import message type '{msg_type_str}' for topic '{topic_name}'")
                continue
            sub = self.create_subscription(
                msg_type,
                topic_name,
                self.topic_callback_factory(topic_name, msg_type_str),
                QoSProfile(depth=10)
            )
            self.subscribers.append(sub)
            self.get_logger().info(f"Subscribed to '{topic_name}' [{msg_type_str}]")

    def topic_callback_factory(self, topic_name, msg_type_str):
        def callback(msg):
            self.get_logger().info(f"Message received on '{topic_name}' [{msg_type_str}]")
        return callback

def main():

    rclpy.init()
    node = Recorder()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        if rclpy.ok():
            node.get_logger().info("Destroying node and shutting down ROS...")
            node.destroy_node()
            rclpy.shutdown()


if __name__ == "__main__":
    main()
