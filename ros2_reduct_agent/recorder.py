import importlib
from collections import defaultdict
from tempfile import SpooledTemporaryFile
from typing import Any, AsyncGenerator

import rclpy
from mcap_ros2.writer import Writer as McapWriter
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.subscription import Subscription
from rclpy.time import Time
from reduct import Client
from rosbag2_py import LocalMessageDefinitionSource

from .config_models import FilenameMode, PipelineConfig, PipelineState, StorageConfig
from .utils import get_or_create_event_loop


class Recorder(Node):
    """ROS2 node that records selected topics to ReductStore."""

    def __init__(self, **kwargs):
        super().__init__(
            "recorder",
            allow_undeclared_parameters=True,
            automatically_declare_parameters_from_overrides=True,
            **kwargs,
        )
        self.logger = self.get_logger()
        self.warned_topics: set[str] = set()

        # Parameters
        self.storage_config = self.load_storage_config()
        self.pipeline_configs = self.load_pipeline_config()

        # ReductStore
        self.client = Client(
            self.storage_config.url, api_token=self.storage_config.api_token
        )
        self.bucket = None
        self.loop = get_or_create_event_loop()
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
    def init_mcap_writers(self):
        """Create an in-memory MCAP writer, per pipeline, a timer that fires
        after max_duration_s, and a callback to upload the MCAP.
        """
        for pipeline_name, cfg in self.pipeline_configs.items():
            duration = cfg.split_max_duration_s
            topics = cfg.include_topics
            max_size = cfg.spool_max_size_bytes
            buffer = SpooledTemporaryFile(max_size=max_size, mode="w+b")
            writer = self.create_mcap_writer(buffer, pipeline_name)

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

            self.logger.info(
                f"[{pipeline_name}] MCAP writer initialized with config:\n"
                f"{cfg.format_for_log()}"
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
        new_writer = self.create_mcap_writer(new_buffer, pipeline_name)

        # Update state with new writer and buffer
        state.buffer = new_buffer
        state.writer = new_writer
        state.current_size = 0
        state.first_timestamp = None
        state.is_uploading = False
        state.timer.reset()

        # Clear and re-register topics and schemas
        state.schema_by_type.clear()
        state.schemas_by_topic.clear()
        self.setup_topic_subscriptions()

        self.logger.debug(
            f"[{pipeline_name}] MCAP writer reset - ready for next segment"
        )

    def create_mcap_writer(
        self, buffer: SpooledTemporaryFile[bytes], pipeline_name: str
    ) -> McapWriter:
        """Create and start an MCAP writer with default compression."""
        chunk_size = self.pipeline_configs[pipeline_name].chunk_size_bytes
        compression = self.pipeline_configs[pipeline_name].compression
        enable_crcs = self.pipeline_configs[pipeline_name].enable_crcs
        return McapWriter(
            buffer,
            chunk_size=chunk_size,
            compression=compression,
            enable_crcs=enable_crcs,
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
                self.logger.warn(f"Skipping '{topic}': No message type found.")
                continue

            msg_type_str = msg_types[0]

            if "/msg/" not in msg_type_str:
                self.logger.warn(
                    f"Skipping '{topic}': Invalid message type format '{msg_type_str}'."
                )
                continue

            self.register_message_schema(topic, msg_type_str)

            if any(sub.topic_name == topic for sub in self.subscribers):
                self.logger.debug(f"Already subscribed to '{topic}'")
                continue

            pkg, msg = msg_type_str.split("/msg/")
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError) as e:
                self.logger.warn(
                    f"Skipping '{topic}': Cannot import '{msg_type_str}' ({e})"
                )
                continue

            sub = self.create_subscription(
                msg_type,
                topic,
                self.make_topic_callback(topic),
                QoSProfile(depth=10),
            )
            self.subscribers.append(sub)
            self.logger.info(f"Subscribed to '{topic}' [{msg_type_str}]")

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema once per message type and associate it with the topic."""
        for state in self.pipeline_states.values():
            if topic_name not in state.topics or topic_name in state.schemas_by_topic:
                continue

            if msg_type_str in state.schema_by_type:
                schema = state.schema_by_type[msg_type_str]
            else:
                source = LocalMessageDefinitionSource()
                msg_def = source.get_full_text(msg_type_str)
                schema = state.writer.register_msgdef(
                    datatype=msg_def.topic_type,
                    msgdef_text=msg_def.encoded_message_definition,
                )
                state.schema_by_type[msg_type_str] = schema
                self.logger.debug(
                    f"[{topic_name}] Registered schema for message type '{msg_type_str}'"
                )

            state.schemas_by_topic[topic_name] = schema

    def make_topic_callback(self, topic_name: str):
        """Generate a callback that writes the message to any relevant pipeline."""

        def _topic_callback(message):
            publish_time = self.get_publish_time(message, topic_name)
            self.process_message(topic_name, message, publish_time)

        return _topic_callback

    def get_publish_time(self, message: Any, topic_name: str) -> int:
        """Extract publish time from message (header.stamp or top-level stamp) in nanoseconds."""
        if hasattr(message, "header") and hasattr(message.header, "stamp"):
            return Time.from_msg(message.header.stamp).nanoseconds
        elif hasattr(message, "stamp"):
            return Time.from_msg(message.stamp).nanoseconds

        if topic_name not in self.warned_topics:
            self.logger.warn(
                f"Message on topic '{topic_name}' has no timestamp. Using current time."
            )
            self.warned_topics.add(topic_name)

        return self.get_clock().now().nanoseconds

    #
    # Message Processing
    #
    def process_message(self, topic_name: str, message: Any, publish_time: int):
        """Process message for all pipelines that include the topic."""
        for pipeline_name, state in self.pipeline_states.items():
            if topic_name not in state.topics:
                continue

            if state.first_timestamp is None:
                state.first_timestamp = publish_time

            self.logger.debug(
                f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
            )

            schema = state.schemas_by_topic.get(topic_name)
            if not schema:
                self.logger.warn(
                    f"[{pipeline_name}] No schema registered for topic '{topic_name}'"
                )
                continue

            state.writer.write_message(
                topic=topic_name,
                schema=schema,
                message=message,
                publish_time=publish_time,
            )
            state.current_size = state.buffer.tell()
            split_size = self.pipeline_configs[pipeline_name].split_max_size_bytes
            if split_size and state.current_size >= split_size:
                self.upload_pipeline(pipeline_name, state)

    #
    # Pipeline Management and Upload
    #
    def make_timer_callback(self, pipeline_name: str):
        """Return a callback that uploads the current pipeline state."""

        def _timer_callback():
            state = self.pipeline_states[pipeline_name]
            self.upload_pipeline(pipeline_name, state)

        return _timer_callback

    def upload_pipeline(self, pipeline_name: str, state: PipelineState):
        """Finish current MCAP, upload if it contains data, and reset writer and state."""
        if not all([state.writer, state.buffer, state.timer]):
            self.logger.warn(
                f"[{pipeline_name}] Incomplete state (writer/buffer/timer) — skipping upload."
            )
            return

        if state.is_uploading:
            self.logger.warn(
                f"[{pipeline_name}] Upload already in progress — skipping upload."
            )
            return

        if state.current_size == 0:
            self.logger.info(
                f"[{pipeline_name}] No new data since last upload — skipping upload."
            )
            state.timer.reset()
            return

        # Prevent concurrent uploads between timer and max size triggers
        state.is_uploading = True

        # Filename index either by increment or timestamp in microseconds
        filename_mode = self.pipeline_configs[pipeline_name].filename_mode
        file_index = (
            state.increment
            if filename_mode == FilenameMode.INCREMENTAL
            else (state.first_timestamp or self.get_clock().now().nanoseconds) // 1_000
        )

        # Finish the MCAP writer
        state.writer.finish()

        # Upload and reset the state
        self.upload_mcap(pipeline_name, state.buffer, file_index)
        self.reset_pipeline_state(pipeline_name, state)
        state.increment += 1

    def upload_mcap(
        self, pipeline_name: str, buffer: SpooledTemporaryFile[bytes], file_index: int
    ):
        """Upload MCAP to ReductStore."""
        self.logger.info(f"[{pipeline_name}] MCAP ready. Uploading to ReductStore...")

        try:
            self.loop.run_until_complete(
                self.upload_to_reductstore(pipeline_name, buffer, file_index)
            )
        except Exception as exc:
            self.logger.error(f"[{pipeline_name}] Failed to upload MCAP: {exc}")

    async def upload_to_reductstore(
        self, pipeline_name: str, buffer: SpooledTemporaryFile[bytes], file_index: int
    ):
        """Asynchronously upload the MCAP file to ReductStore."""
        content_length = buffer.tell()
        buffer.seek(0)

        await self.bucket.write(
            pipeline_name,
            self.read_in_chunks(buffer),
            file_index,
            content_length,
            content_type="application/mcap",
        )

    async def read_in_chunks(
        self, buffer: SpooledTemporaryFile[bytes], chunk_size: int = 100_000
    ) -> AsyncGenerator[bytes, None]:
        """Yield chunks from the buffer."""
        while True:
            chunk = buffer.read(chunk_size)
            if not chunk:
                break
            yield chunk


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
