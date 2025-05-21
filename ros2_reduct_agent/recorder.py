import asyncio
import importlib
from collections import defaultdict
from contextlib import contextmanager
from io import BytesIO
from time import time
from typing import Any

import rclpy
from mcap.writer import CompressionType
from mcap.writer import Writer as McapWriter
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.serialization import serialize_message
from rclpy.subscription import Subscription
from reduct import Client

from .config_models import FilenameMode, PipelineConfig, PipelineState, StorageConfig
from .utils import get_message_schema


class Recorder(Node):

    @staticmethod
    @contextmanager
    def uploading_lock(state, logger=None, pipeline_name=None, skip_message=None):
        if getattr(state, "is_uploading", False):
            if logger and pipeline_name and skip_message:
                logger.warn(f"[{pipeline_name}] {skip_message}")
            yield False
            return
        state.is_uploading = True
        try:
            yield True
        finally:
            state.is_uploading = False

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

        self.pipeline_states: dict[str, PipelineState] = {}
        self.subscribers: list[Subscription] = []

        self.init_mcap_writers()
        self.setup_topic_subscriptions()

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

    def validate_pipeline_config(self) -> dict[str, PipelineConfig]:
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

    def _create_mcap_writer(self, buffer: BytesIO) -> McapWriter:
        """Create and start an MCAP writer with the configured compression."""
        writer = McapWriter(buffer, compression=CompressionType.NONE)
        writer.start()
        return writer

    def init_mcap_writers(self):
        """Create an in-memory MCAP writer, per pipeline, a timer that fires
        after ``max_duration_s`` or when the size exceeds ``max_size_bytes``.
        """
        for pipeline_name, cfg in self.pipelines.items():
            duration = cfg.split_max_duration_s
            topics = cfg.include_topics
            filename_mode = cfg.filename_mode

            buffer = BytesIO()
            writer = self._create_mcap_writer(buffer)

            state = PipelineState(
                topics=topics,
                buffer=buffer,
                writer=writer,
            )
            self.pipeline_states[pipeline_name] = state

            timer = self.create_timer(
                float(duration),
                self.make_pipeline_callback(pipeline_name, filename_mode),
            )
            state.timer = timer

            self.get_logger().info(
                f"[{pipeline_name}] MCAP writer initialised (every {duration}s) with topics: {state.topics}"
            )

    def make_pipeline_callback(self, pipeline_name: str, filename_mode: FilenameMode):
        """Return a closure that finalises and handles the MCAP segment."""

        def _pipeline_callback():
            state = self.pipeline_states[pipeline_name]
            with self.uploading_lock(
                state,
                logger=self.get_logger(),
                pipeline_name=pipeline_name,
                skip_message="Timer skipped: upload in progress",
            ) as acquired:
                if not acquired:
                    return

                writer = state.writer
                buffer = state.buffer
                timer = state.timer
                timestamp = (
                    state.counter
                    if filename_mode == FilenameMode.COUNTER
                    else state.timestamp
                )

                if any(
                    [writer is None, buffer is None, timestamp is None, timer is None]
                ):
                    self.get_logger().warn(
                        f"[{pipeline_name}] Missing required state - skipping upload."
                    )
                    return

                writer.finish()
                buffer.seek(0)
                data = buffer.read()

                self.upload_mcap(pipeline_name, data, timestamp)

                new_buffer = BytesIO()
                new_writer = self._create_mcap_writer(new_buffer)
                state.current_size = 0
                state.counter += 1
                timer.reset()

                state.buffer = new_buffer
                state.writer = new_writer
                state.channels = {}
                self.get_logger().info(
                    f"[{pipeline_name}] MCAP writer reset - ready for next segment"
                )

        return _pipeline_callback

    def upload_mcap(self, pipeline_name: str, data: bytes, timestamp: int):
        """Upload an MCAP segment to ReductStore."""
        self.get_logger().info(
            f"[{pipeline_name}] MCAP segment ready. Uploading to ReductStore..."
        )

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
        topics_to_subscribe = {
            t for p in self.pipelines.values() for t in p.include_topics
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

            sub = self.create_subscription(
                msg_type,
                topic,
                self.topic_callback_factory(topic, msg_type_str),
                QoSProfile(depth=10),
            )
            self.subscribers.append(sub)
            self.get_logger().info(f"Subscribed to '{topic}' [{msg_type_str}]")

    def topic_callback_factory(self, topic_name: str, msg_type_str: str):
        """Generate a callback that writes the message to any relevant pipeline MCAP."""

        def _callback(msg):
            try:
                serialized = serialize_message(msg)
            except Exception as exc:
                self.get_logger().error(
                    f"Failed to serialise message on '{topic_name}': {exc}"
                )
                return

            if hasattr(msg, "header") and hasattr(msg.header, "stamp"):
                log_time = int(
                    msg.header.stamp.sec * 1_000_000 + msg.header.stamp.nanosec // 1_000
                )
            else:
                log_time = int(time() * 1_000_000)
                self.get_logger().warn(
                    f"Message on '{topic_name}' has no header.stamp, using current time: {log_time}"
                )

            for pipeline_name, state in self.pipeline_states.items():
                if topic_name not in state.topics:
                    continue

                if state.timestamp is None:
                    state.timestamp = log_time

                state.current_size += len(serialized)

                if (
                    self.pipelines[pipeline_name].split_max_size_bytes
                    and state.current_size
                    > self.pipelines[pipeline_name].split_max_size_bytes
                ):
                    with self.uploading_lock(
                        state,
                        logger=self.get_logger(),
                        pipeline_name=pipeline_name,
                        skip_message="Size flush skipped: upload in progress",
                    ) as acquired:
                        if not acquired:
                            return

                        self.get_logger().warn(
                            f"Pipeline '{pipeline_name}' exceeded max size, resetting writer."
                        )
                        state.timer.reset()
                        state.writer.finish()
                        state.buffer.seek(0)
                        data = state.buffer.read()
                        self.upload_mcap(pipeline_name, data, log_time)
                        state.buffer = BytesIO()
                        state.writer = self._create_mcap_writer(state.buffer)
                        state.current_size = 0

                self.get_logger().info(
                    f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
                )

                writer: McapWriter = state.writer
                channels = state.channels
                schemas = state.schemas

                channel_id = channels.get(topic_name)
                if channel_id is None:
                    schema_id = schemas.get(msg_type_str)
                    if schema_id is None:
                        try:
                            schema_data = get_message_schema(msg_type_str)
                        except Exception as exc:
                            self.get_logger().warn(
                                f"Could not generate schema for '{msg_type_str}': {exc}"
                            )
                            schema_data = b""
                        schema_id = writer.register_schema(
                            name=msg_type_str,
                            encoding="ros2msg",
                            data=schema_data,
                        )
                        schemas[msg_type_str] = schema_id
                        state.schemas = schemas

                    channel_id = writer.register_channel(
                        topic=topic_name,
                        message_encoding="cdr",
                        schema_id=schema_id,
                    )
                    channels[topic_name] = channel_id

                writer.add_message(
                    channel_id=channel_id,
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
