import asyncio
import importlib
import time
from collections import defaultdict
from tempfile import SpooledTemporaryFile
from typing import Any

import rclpy
from mcap.writer import CompressionType
from mcap_ros2.writer import Writer as McapWriter
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.subscription import Subscription
from rclpy.time import Time
from reduct import Client

from .config_models import FilenameMode, PipelineConfig, PipelineState, StorageConfig
from .utils import get_message_schema


class Recorder(Node):
    """ROS2 node that records selected topics to ReductStore."""

    def __init__(self, **kwargs):
        super().__init__(
            "recorder",
            allow_undeclared_parameters=True,
            automatically_declare_parameters_from_overrides=True,
            **kwargs,
        )
        # Parameters
        self.storage_config = self.load_storage_config()
        self.pipeline_configs = self.load_pipeline_config()

        # ReductStore
        self.client = Client(
            self.storage_config.url, api_token=self.storage_config.api_token
        )
        self.bucket = None
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.init_reduct_bucket())

        # Pipelines
        self.pipeline_states: dict[str, PipelineState] = {}
        self.subscribers: list[Subscription] = []
        self.init_mcap_writers()
        self.setup_topic_subscriptions()

    def load_storage_config(self) -> StorageConfig:
        """Parse and validate storage parameters."""
        params = {}
        for key in ["url", "api_token", "bucket"]:
            param = f"storage.{key}"
            if not self.has_parameter(param):
                raise ValueError(f"Missing parameter: '{param}'")
            params[key] = self.get_parameter(param).value
        return StorageConfig(**params)

    def load_pipeline_config(self) -> dict[str, PipelineConfig]:
        """Parse and validate pipeline parameters."""
        pipelines_raw: dict[str, dict[str, Any]] = defaultdict(dict)
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

        pipelines: dict[str, PipelineConfig] = {}
        for name, cfg in pipelines_raw.items():
            pipelines[name] = PipelineConfig(**cfg)
        return pipelines

    async def init_reduct_bucket(self):
        """Initialize or create ReductStore bucket."""
        self.bucket = await self.client.create_bucket(
            self.storage_config.bucket, exist_ok=True
        )

    #
    # MCAP Management
    #
    def create_mcap_writer(self, buffer: SpooledTemporaryFile[bytes]) -> McapWriter:
        """Create and start an MCAP writer with default compression."""
        return McapWriter(buffer, compression=CompressionType.ZSTD)

    def init_mcap_writers(self):
        """Create an in-memory MCAP writer, per pipeline, a timer that fires
        after max_duration_s, and a callback to upload the MCAP.
        """
        for pipeline_name, cfg in self.pipeline_configs.items():
            duration = cfg.split_max_duration_s
            topics = cfg.include_topics
            max_size = cfg.spool_max_size_bytes
            buffer = SpooledTemporaryFile(max_size=max_size, mode="w+b")
            writer = self.create_mcap_writer(buffer)

            state = PipelineState(
                topics=topics,
                buffer=buffer,
                writer=writer,
            )
            self.pipeline_states[pipeline_name] = state

            timer = self.create_timer(
                float(duration),
                self.make_timer_callback(pipeline_name),
            )
            state.timer = timer

            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer initialised (every {duration}s) with topics: {state.topics}"
            )

    def reset_pipeline_state(
        self,
        pipeline_name: str,
        state: PipelineState,
    ):
        """Finish current MCAP, upload it, and reset writer and buffer."""
        cfg = self.pipeline_configs[pipeline_name]
        max_size = cfg.spool_max_size_bytes
        new_buffer = SpooledTemporaryFile(max_size=max_size, mode="w+b")
        new_writer = self.create_mcap_writer(new_buffer)
        state.buffer = new_buffer
        state.writer = new_writer
        state.current_size = 0
        state.increment += 1
        state.first_time = None
        state.timer.reset()
        state.is_uploading = False
        self.get_logger().info(
            f"[{pipeline_name}] MCAP writer reset - ready for next segment"
        )

    #
    # Topic Subscription
    #
    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by any pipeline."""
        topics_to_subscribe = {
            t for p in self.pipeline_configs.values() for t in p.include_topics
        }
        topic_types = dict(self.get_topic_names_and_types())

        for topic in topics_to_subscribe:
            msg_types = topic_types.get(topic)
            if not msg_types:
                self.get_logger().warn(f"Skipping '{topic}': No message type found.")
                continue

            msg_type_str = msg_types[0]
            if "/msg/" not in msg_type_str:
                self.get_logger().warn(
                    f"Skipping '{topic}': Invalid message type format '{msg_type_str}'."
                )
                continue

            pkg, msg = msg_type_str.split("/msg/")
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError) as e:
                self.get_logger().warn(
                    f"Skipping '{topic}': Cannot import '{msg_type_str}' ({e})"
                )
                continue

            self.register_message_schema(topic, msg_type_str)

            sub = self.create_subscription(
                msg_type,
                topic,
                self.make_topic_callback(topic),
                QoSProfile(depth=10),
            )
            self.subscribers.append(sub)
            self.get_logger().info(f"Subscribed to '{topic}' [{msg_type_str}]")

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register message schema for the given topic in all pipelines."""
        for state in self.pipeline_states.values():
            if topic_name in state.topics and topic_name not in state.schemas:
                msgdef_text = get_message_schema(msg_type_str)
                schema = state.writer.register_msgdef(
                    datatype=msg_type_str,
                    msgdef_text=msgdef_text,
                )
                state.schemas[topic_name] = schema

    def make_topic_callback(self, topic_name: str):
        """Generate a callback that writes the message to any relevant pipeline."""

        def _topic_callback(message):
            publish_time = self.get_publish_time(message, topic_name)
            self.process_message(topic_name, message, publish_time)

        return _topic_callback

    def get_publish_time(self, message: Any, topic_name: str) -> int:
        """Extract publish time from message header in nanoseconds."""
        if hasattr(message, "header") and hasattr(message.header, "stamp"):
            return int(Time.from_msg(message).nanoseconds)
        self.get_logger().warn(
            f"Message on '{topic_name}' has no header.stamp, using current time."
        )
        return time.time_ns()

    #
    # Message Processing
    #
    def process_message(self, topic_name: str, message: Any, publish_time: int):
        """Process serialized message for all pipelines that include the topic."""
        for pipeline_name, state in self.pipeline_states.items():
            if topic_name not in state.topics:
                continue

            if state.first_time is None:
                state.first_time = publish_time

            self.get_logger().info(
                f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
            )

            schema = state.schemas[topic_name]
            state.writer.write_message(
                topic=topic_name,
                schema=schema,
                message=message,
                publish_time=publish_time,
            )
            state.current_size += self.estimate_message_size(message)
            split_size = self.pipeline_configs[pipeline_name].split_max_size_bytes
            if split_size and state.current_size > split_size:
                self.upload_pipeline(pipeline_name, state)

    def estimate_message_size(self, message: Any) -> int:
        """Estimate the size of a message in bytes without serialization."""
        return len(message.data) if hasattr(message, "data") else 0

    #
    # Pipeline Management and Upload
    #
    def make_timer_callback(self, pipeline_name: str):
        """Return a callback that uploads the current pipeline state."""

        def _timer_callback():
            state = self.pipeline_states[pipeline_name]
            self.upload_pipeline(pipeline_name, state)

        return _timer_callback

    def upload_pipeline(
        self,
        pipeline_name: str,
        state: PipelineState,
    ):
        """Finish current MCAP, upload, and reset writer and state."""
        if any(
            x is None
            for x in (
                state.writer,
                state.buffer,
                state.timer,
            )
        ):
            self.get_logger().warn(
                f"[{pipeline_name}] Missing required state - skipping upload."
            )
            return

        if state.is_uploading:
            self.get_logger().warn(
                f"[{pipeline_name}] Upload already in progress - skipping upload."
            )
            return

        state.is_uploading = True
        filename_mode = self.pipeline_configs[pipeline_name].filename_mode
        file_index = (
            state.increment
            if filename_mode == FilenameMode.INCREMENTAL
            else (state.first_time or time.time_ns()) // 1_000
        )
        state.writer.finish()
        state.buffer.seek(0)
        data = state.buffer.read()
        self.upload_mcap(pipeline_name, data, file_index)
        self.reset_pipeline_state(pipeline_name, state)

    def upload_mcap(self, pipeline_name: str, data: bytes, file_index: int):
        """Upload MCAP to ReductStore."""
        self.get_logger().info(
            f"[{pipeline_name}] MCAP segment ready. Uploading to ReductStore..."
        )

        async def _upload():
            self.get_logger().info(
                f"[{pipeline_name}] Uploading MCAP segment to ReductStore entry '{pipeline_name}' at {file_index}..."
            )
            await self.bucket.write(
                pipeline_name, data, file_index, content_type="application/mcap"
            )
            self.get_logger().info(
                f"[{pipeline_name}] MCAP segment uploaded successfully."
            )

        try:
            self.loop.run_until_complete(_upload())
        except Exception as exc:
            self.get_logger().error(
                f"[{pipeline_name}] Failed to upload MCAP segment: {exc}"
            )


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
