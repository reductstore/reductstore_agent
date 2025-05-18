import asyncio
import importlib
from collections import defaultdict
from io import BytesIO
from typing import Any, Dict

import rclpy
from mcap.writer import Writer as McapWriter
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import serialize_message
from reduct import Client

from .config_models import PipelineConfig, StorageConfig


class Recorder(Node):
    """ROS2 node that records selected topics to ReductStore."""

    def __init__(self, **kwargs):
        super().__init__(
            "recorder",
            allow_undeclared_parameters=True,
            automatically_declare_parameters_from_overrides=True,
            **kwargs,
        )
        self.storage = self.validate_storage_config()
        self.pipelines = self.validate_pipeline_config()

        self.client = Client(self.storage.url, api_token=self.storage.api_token)
        self.bucket = None
        asyncio.get_event_loop().run_until_complete(self._init_reduct_bucket())

        self.mcap_pipelines = {}
        self.subscribers = []

        self.init_mcap_writers()
        self.setup_topic_subscriptions()

        # Counter for debugging
        self.counter = 0

    async def _init_reduct_bucket(self):
        self.bucket = await self.client.create_bucket(
            self.storage.bucket, exist_ok=True
        )

    def validate_storage_config(self) -> StorageConfig:
        """Load and validate required storage parameters."""
        params = {}
        for key in ["url", "api_token", "bucket"]:
            param = f"storage.{key}"
            if not self.has_parameter(param):
                raise ValueError(f"Missing parameter: '{param}'")
            value = self.get_parameter(param).value
            params[key] = value
        return StorageConfig(**params)

    def validate_pipeline_config(self) -> Dict[str, PipelineConfig]:
        """Parse and validate pipeline parameters."""
        pipelines_raw: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for param in self.get_parameters_by_prefix("pipelines").values():
            name = param.name
            value = param.value
            parts = name.split(".")
            if len(parts) < 3:
                raise ValueError(
                    (
                        f"Invalid pipeline parameter name: '{name}'. "
                        "Expected 'pipelines.<pipeline_name>.<subkey>'"
                    )
                )
            pipeline_name = parts[1]
            subkey = ".".join(parts[2:])
            pipelines_raw[pipeline_name][subkey] = value

        pipelines: Dict[str, PipelineConfig] = {}
        for name, cfg in pipelines_raw.items():
            pipelines[name] = PipelineConfig(**cfg)
        return pipelines

    def init_mcap_writers(self):
        """Create an in-memory MCAP writer, per pipeline, and a timer that fires
        after ``max_duration_s`` to store the segment in ReductStore.
        """
        for pipeline_name, cfg in self.pipelines.items():
            duration = cfg.split_max_duration_s
            topics = cfg.include_topics

            buffer = BytesIO()
            writer = McapWriter(buffer)
            writer.start()

            self.mcap_pipelines[pipeline_name] = {
                "buffer": buffer,
                "writer": writer,
                "channels": {},
                "topics": topics,
            }

            timer = self.create_timer(
                float(duration), self.make_pipeline_callback(pipeline_name)
            )
            self.mcap_pipelines[pipeline_name]["timer"] = timer

            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer initialised (every {duration}s) with topics: {self.mcap_pipelines[pipeline_name]['topics']}"
            )

    def make_pipeline_callback(self, pipeline_name):
        """Return a closure that finalises and handles the MCAP segment."""

        def _pipeline_callback():
            state = self.mcap_pipelines[pipeline_name]
            writer: McapWriter = state["writer"]
            buffer: BytesIO = state["buffer"]

            writer.finish()
            buffer.seek(0)
            data = buffer.read()

            self.upload_mcap(pipeline_name, data)

            new_buffer = BytesIO()
            new_writer = McapWriter(new_buffer)
            new_writer.start()

            state["buffer"] = new_buffer
            state["writer"] = new_writer
            state["channels"] = {}
            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer reset - ready for next segment"
            )

        return _pipeline_callback

    def upload_mcap(self, pipeline_name: str, data: bytes):
        """Upload an MCAP segment to ReductStore."""
        self.get_logger().info(
            f"[{pipeline_name}] MCAP segment ready. Uploading to ReductStore..."
        )

        # TODO: use message time instead
        timestamp = self.counter
        self.counter += 1

        async def _upload():
            await self.bucket.write(
                pipeline_name, data, timestamp, content_type="application/mcap"
            )
            self.get_logger().info(
                f"[{pipeline_name}] Uploaded MCAP segment to ReductStore entry '{pipeline_name}' at {timestamp} ms."
            )

        try:
            asyncio.get_event_loop().run_until_complete(_upload())
        except Exception as exc:
            self.get_logger().error(
                f"[{pipeline_name}] Failed to upload MCAP segment: {exc}"
            )

    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by any pipeline."""
        topics_to_subscribe = set()
        for pipeline in self.pipelines.values():
            for t in getattr(pipeline, "include_topics", []):
                if isinstance(t, dict):
                    name = t.get("name")
                    msg_type_str = t.get("type")
                else:
                    name = t
                    msg_type_str = None
                if name:
                    topics_to_subscribe.add((name, msg_type_str))

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

    def topic_callback_factory(self, topic_name: str, msg_type_str: str):
        """Generate a callback that writes the message to any relevant pipeline MCAP."""

        def _callback(msg):
            self.get_logger().debug(
                f"Message received on '{topic_name}' [{msg_type_str}]"
            )

            try:
                serialized = serialize_message(msg)
            except Exception as exc:
                self.get_logger().error(
                    f"Failed to serialise message on '{topic_name}': {exc}"
                )
                return

            log_time = self.get_clock().now().nanoseconds  # TODO: use ROS2 time

            for pipeline_name, state in self.mcap_pipelines.items():
                if topic_name not in state["topics"]:
                    continue

                self.get_logger().info(
                    f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
                )

                writer: McapWriter = state["writer"]
                channels = state["channels"]

                if topic_name not in channels:
                    schema_id = writer.register_schema(
                        name=msg_type_str,
                        encoding="ros2msg",  # TODO: unknown encoding
                        data=b"",  # TODO: unknown schema
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
