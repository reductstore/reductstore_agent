from typing import Any
from collections import defaultdict

import rclpy
from rclpy.node import Node
from std_msgs.msg import Int32
from std_srvs.srv import SetBool


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
        """Sets up subscribers to configure the node"""

        # subscriber for handling incoming messages
        self.subscriber = self.create_subscription(
            Int32, "~/input", self.topic_callback, qos_profile=10
        )
        self.get_logger().info(f"Subscribed to '{self.subscriber.topic_name}'")

        # service server for handling service calls
        self.service_server = self.create_service(
            SetBool, "~/service", self.service_callback
        )

        # timer for repeatedly invoking a callback to publish messages
        self.timer = self.create_timer(5.0, self.timer_callback)

    def topic_callback(self, msg: Int32):
        """Processes messages received by a subscriber

        Args:
            msg (Int32): message
        """

        self.get_logger().info(f"Message received: '{msg.data}'")

    def service_callback(
        self, request: SetBool.Request, response: SetBool.Response
    ) -> SetBool.Response:
        """Processes service requests

        Args:
            request (SetBool.Request): service request
            response (SetBool.Response): service response

        Returns:
            SetBool.Response: service response
        """

        self.get_logger().info("Received service request")
        response.success = True

        return response

    def timer_callback(self):
        """Processes timer triggers"""

        self.get_logger().info("Timer triggered")


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
