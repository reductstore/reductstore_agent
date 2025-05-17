import asyncio
import importlib
import io
from collections import defaultdict
from typing import Any, Dict

import rclpy
from mcap.writer import Writer as McapWriter
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import serialize_message
from reduct import Client


class Recorder(Node):
    """ROS 2 node that records selected topics into in-memory MCAP segments, one per
    configured pipeline. Each segment is finalised and passed to ``handle_mcap``
    after ``max_duration_s``.
    """

    def __init__(self, **kwargs):
        super().__init__(
            "recorder",
            allow_undeclared_parameters=True,
            automatically_declare_parameters_from_overrides=True,
            **kwargs,
        )

        # Parameter-driven configuration
        self.storage = self.load_and_validate_storage_config()
        self.pipelines: Dict[str, Dict[str, Any]] = (
            self.parse_and_validate_pipeline_config()
        )

        # ReductStore client setup
        self.client = Client(self.storage["url"], api_token=self.storage["api_token"])
        self.bucket = None
        asyncio.get_event_loop().run_until_complete(self._init_reduct_bucket())

        # Runtime structures (initialised below)
        self.mcap_pipelines: Dict[str, Dict[str, Any]] = {}
        self.subscribers = []

        # Counter for debugging
        self.counter = 0

        # Prepare runtime helpers
        self.init_mcap_writers()
        self.setup_topic_subscriptions()

    async def _init_reduct_bucket(self):
        self.bucket = await self.client.create_bucket(
            self.storage["bucket"], exist_ok=True
        )

    def load_and_validate_storage_config(self) -> Dict[str, Any]:
        """Load and validate required storage parameters."""
        required_keys = ["url", "api_token", "bucket"]
        config: Dict[str, Any] = {}

        for key in required_keys:
            param = f"storage.{key}"
            if not self.has_parameter(param):
                raise SystemExit(f"Missing parameter: '{param}'")

            value = self.get_parameter(param).value
            if not value:
                raise SystemExit(f"Empty value for parameter: '{param}'")

            config[key] = value

        return config

    def parse_and_validate_pipeline_config(self) -> Dict[str, Dict[str, Any]]:
        """Parse ``pipelines.*`` parameters and validate their values."""
        pipelines: Dict[str, Dict[str, Any]] = defaultdict(dict)

        for param in self._parameters.values():
            name = param.name
            value = param.value

            if not name.startswith("pipelines."):
                continue

            parts = name.split(".")
            if len(parts) < 3:
                raise SystemExit(
                    f"Invalid pipeline parameter name: '{name}'. Expected 'pipelines.<pipeline_name>.<subkey>'"
                )

            pipeline_name = parts[1]
            subkey = ".".join(parts[2:])
            pipelines[pipeline_name][subkey] = value

            self.validate_pipeline_parameter(name, value)

        return pipelines

    def validate_pipeline_parameter(self, name: str, value: Any):
        if name.endswith("max_duration_s"):
            if not isinstance(value, int) or not (1 <= value <= 3600):
                raise SystemExit(
                    f"'{name}' should be an int between 1 and 3600s. Got: {value}"
                )

        elif name.endswith("max_size_bytes"):
            if not isinstance(value, int) or not (1_000 <= value <= 1_000_000_000):
                raise SystemExit(
                    f"'{name}' should be an int between 1KB and 1GB. Got: {value}"
                )

        elif name.endswith("include_topics"):
            if not isinstance(value, list) or not all(
                isinstance(v, str) and v.startswith("/") for v in value
            ):
                raise SystemExit(
                    f"'{name}' should be a list of ROS topic names starting with '/'. Got: {value}"
                )

    # ---------------------------------------------------------------------
    # MCAP helpers
    # ---------------------------------------------------------------------
    def init_mcap_writers(self):
        """Create an in-memory MCAP writer, per pipeline, and a timer that fires
        after ``max_duration_s`` to off-load the segment.
        """
        for pipeline_name, cfg in self.pipelines.items():
            duration = cfg.get("split.max_duration_s", 60)

            buffer = io.BytesIO()
            writer = McapWriter(buffer)
            writer.start()

            self.mcap_pipelines[pipeline_name] = {
                "buffer": buffer,
                "writer": writer,
                "channels": {},
                "topics": {
                    (t if isinstance(t, str) else t.get("name"))
                    for t in cfg.get("include_topics", [])
                    if t
                },
            }

            # Periodic flush timer
            timer = self.create_timer(
                float(duration), self.make_pipeline_flush_callback(pipeline_name)
            )
            self.mcap_pipelines[pipeline_name]["timer"] = timer

            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer initialised (flush every {duration}s) with topics: {self.mcap_pipelines[pipeline_name]['topics']}"
            )

    def make_pipeline_flush_callback(self, pipeline_name):
        """Return a closure that finalises and handles the MCAP segment."""

        def _flush():
            state = self.mcap_pipelines[pipeline_name]
            writer: McapWriter = state["writer"]
            buffer: io.BytesIO = state["buffer"]

            # Finalise the current segment
            writer.finish()
            buffer.seek(0)
            data = buffer.read()

            self.handle_mcap(pipeline_name, data)

            # Reset buffer & writer for the next segment
            new_buffer = io.BytesIO()
            new_writer = McapWriter(new_buffer)
            new_writer.start()

            state["buffer"] = new_buffer
            state["writer"] = new_writer
            state["channels"] = {}
            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer reset - ready for next segment"
            )

        return _flush

    def handle_mcap(self, pipeline_name: str, data: bytes):
        """Handle a completed MCAP segment.

        Upload the MCAP segment to ReductStore using the reduct-py SDK.
        """
        self.get_logger().info(
            f"[{pipeline_name}] MCAP segment ready. Uploading to ReductStore..."
        )

        # TODO: use message time instead
        timestamp = self.counter
        self.counter += 1

        # Use asyncio to upload
        async def upload():
            await self.bucket.write(
                pipeline_name, data, timestamp, content_type="application/mcap"
            )
            self.get_logger().info(
                f"[{pipeline_name}] Uploaded MCAP segment to ReductStore entry '{pipeline_name}' at {timestamp} ms."
            )

        try:
            asyncio.get_event_loop().run_until_complete(upload())
        except Exception as exc:
            self.get_logger().error(
                f"[{pipeline_name}] Failed to upload MCAP segment: {exc}"
            )

    # ---------------------------------------------------------------------
    # Topic subscription helpers
    # ---------------------------------------------------------------------
    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by any pipeline."""
        # ignore_topics = {"/rosout", "/parameter_events"}

        # Build set of (topic, msg_type_str)
        topics_to_subscribe = set()
        for pipeline in self.pipelines.values():
            for t in pipeline.get("include_topics", []):
                if isinstance(t, dict):
                    name = t.get("name")
                    msg_type_str = t.get("type")
                else:
                    name = t
                    msg_type_str = None
                if name:  # and name not in ignore_topics
                    topics_to_subscribe.add((name, msg_type_str))

        # Query ROS graph for topic types
        topic_types = dict(self.get_topic_names_and_types())

        for topic_name, msg_type_str in topics_to_subscribe:
            # Infer message type if not provided
            if not msg_type_str:
                types = topic_types.get(topic_name)
                if not types:
                    self.get_logger().warn(
                        f"No type found for topic '{topic_name}' - skipping."
                    )
                    continue
                msg_type_str = types[0]

            pkg, _, msg = msg_type_str.partition("/msg/")
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError):
                self.get_logger().warn(
                    f"Cannot import message type '{msg_type_str}' for topic '{topic_name}'"
                )
                continue

            sub = self.create_subscription(
                msg_type,
                topic_name,
                self.topic_callback_factory(topic_name, msg_type_str),
                QoSProfile(depth=10),
            )
            self.subscribers.append(sub)
            self.get_logger().info(f"Subscribed to '{topic_name}' [{msg_type_str}]")

    # ---------------------------------------------------------------------
    # Per-message callback (wrapped via factory)
    # ---------------------------------------------------------------------
    def topic_callback_factory(self, topic_name: str, msg_type_str: str):
        """Generate a callback that writes the message to any relevant pipeline MCAP."""

        def _callback(msg):
            self.get_logger().debug(
                f"Message received on '{topic_name}' [{msg_type_str}]"
            )

            # Serialise once, reuse for all pipelines that include this topic
            try:
                serialized = serialize_message(msg)
            except Exception as exc:  # noqa: BLE001
                self.get_logger().error(
                    f"Failed to serialise message on '{topic_name}': {exc}"
                )
                return

            log_time = self.get_clock().now().nanoseconds  # TODO: use ROS2 time

            for pipeline_name, state in self.mcap_pipelines.items():
                if topic_name not in state["topics"]:
                    continue  # not part of this pipeline

                self.get_logger().info(
                    f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
                )

                writer: McapWriter = state["writer"]
                channels = state["channels"]

                # Lazily create channel (and dummy schema) the first time we see this topic
                if topic_name not in channels:
                    schema_id = writer.register_schema(
                        name=msg_type_str,
                        encoding="ros2msg",
                        data=b"",  # minimal / unknown schema
                    )
                    channel_id = writer.register_channel(
                        topic=topic_name,
                        message_encoding="cdr",  # TODO: use correct encoding
                        schema_id=schema_id,
                    )
                    channels[topic_name] = channel_id

                writer.add_message(
                    channel_id=channels[topic_name],
                    log_time=log_time,
                    data=serialized,
                    publish_time=log_time,
                )

        return _callback


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
