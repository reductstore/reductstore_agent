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

"""ROS2 ReductStore Recorder Node."""

import importlib
import re
from collections import defaultdict
from typing import Any

import rclpy
import yaml
from rclpy.impl.logging_severity import LoggingSeverity
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.subscription import Subscription
from rclpy.time import Time
from reduct import BucketSettings, Client

from .downsampler import Downsampler
from .models import OutputFormat, PipelineConfig, RemoteConfig, StorageConfig
from .state import PipelineState
from .utils import get_or_create_event_loop
from .writer import create_writer


class Recorder(Node):
    """ROS2 node that records selected topics to ReductStore."""

    def __init__(self, **kwargs):
        """Initialize the Recorder node and set up pipelines and storage."""
        super().__init__(
            "recorder",
            allow_undeclared_parameters=True,
            automatically_declare_parameters_from_overrides=True,
            **kwargs,
        )
        self.logger = self.get_logger()
        self.warned_topics: set[str] = set()
        self.loop = get_or_create_event_loop()

        # Parameters
        self.storage_config = self.load_storage_config()
        self.pipeline_configs = self.load_pipeline_config()
        self.remote_config = self.load_remote_config()

        # Pipeline States
        self.pipeline_states: dict[str, PipelineState] = {}
        self.subscribers: list[Subscription] = []

        if self.remote_config and self.pipeline_configs:
            raise ValueError(
                "Cannot have both remote configuration "
                "and local pipeline configuration."
            )
        if not self.pipeline_configs and not self.remote_config:
            raise ValueError("No configuration found.")

        # ReductStore
        self.client = Client(
            self.storage_config.url, api_token=self.storage_config.api_token
        )
        self.bucket = None
        self.loop.run_until_complete(self.init_reduct_bucket())
        if self.remote_config:
            self.log_info(lambda: "Configuration management enabled.")
            try:
                self.loop.run_until_complete(self.check_remote_updates())
            except Exception as exc:
                self.log_warn(
                    lambda exc=exc: "Failed to check remote updates: "
                    f"{exc}, loading backup."
                )
                self.load_backup_configuration()

        # Pipeline
        if not self.remote_config:
            self.log_info(lambda: "Using local pipeline configuration.")
            for name, cfg in self.pipeline_configs.items():
                self.init_pipeline_writer(name, cfg)

        # Delay topic subscriptions
        delay = self.load_delay_config()
        if delay <= 0.0:
            self.setup_topic_subscriptions()
            self.start_pipeline_timers()
            return

        def _delayed_setup():
            self.setup_topic_subscriptions()
            self.start_pipeline_timers()
            self.destroy_timer(timer)

        timer = self.create_timer(delay, _delayed_setup)
        if self.remote_config:

            def pull_timer():
                self.log_info(lambda: "Remote config pull timer fired.")
                self.loop.create_task(self.check_remote_updates())

            self._remote_config_timer = self.create_timer(
                timer_period_sec=self.remote_config.pull_frequency_s,
                callback=pull_timer,
                autostart=True,
            )

    def log_info(self, msg_fn):
        """Log an info message if enabled."""
        if self.logger.is_enabled_for(LoggingSeverity.INFO):
            self.logger.info(msg_fn())

    def log_debug(self, msg_fn):
        """Log a debug message if enabled."""
        if self.logger.is_enabled_for(LoggingSeverity.DEBUG):
            self.logger.debug(msg_fn())

    def log_warn(self, msg_fn):
        """Log a warning message if enabled."""
        if self.logger.is_enabled_for(LoggingSeverity.WARN):
            self.logger.warning(msg_fn())

    def load_storage_config(self) -> StorageConfig:
        """Parse and validate storage parameters."""
        required_keys = ["url", "api_token", "bucket"]
        optional_keys = [
            "quota_type",
            "quota_size",
            "max_block_size",
            "max_block_records",
        ]

        params = {}

        for key in required_keys:
            param = f"storage.{key}"
            if not self.has_parameter(param):
                raise ValueError(f"Missing parameter: '{param}'")
            params[key] = self.get_parameter(param).value

        for key in optional_keys:
            param = f"storage.{key}"
            if self.has_parameter(param):
                params[key] = self.get_parameter(param).value

        return StorageConfig(**params)

    def load_remote_config(self):
        """Parse and validate configuration params."""
        required_keys = ["url", "api_token", "bucket", "entry"]
        optional_keys = ["pull_frequency_s"]

        params = {}

        for key in required_keys:
            param = f"remote.{key}"
            if not self.has_parameter(param):
                self.logger.info("No remote configuration parameters found.")
                return None
            params[key] = self.get_parameter(param).value

        for key in optional_keys:
            param = f"remote.{key}"
            if self.has_parameter(param):
                params[key] = self.get_parameter(param).value

        return RemoteConfig(**params)

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
            if subkey.startswith("static_labels."):
                label_key = subkey.split(".", 1)[1]
                pipelines_raw[pipeline_name].setdefault("static_labels", {})[
                    label_key
                ] = value
            elif subkey.startswith("labels."):
                parts = subkey.split(".")
                if len(parts) < 3:
                    raise ValueError(
                        (
                            f"Invalid label key '{parts} "
                            "Expected 'pipelines.<pipeline_name>.<labels>."
                        )
                    )
                idx_str = parts[1]
                rest_parts = parts[2:]
                try:
                    idx = int(idx_str)
                except ValueError:
                    continue
                labels_dict = pipelines_raw[pipeline_name].setdefault("labels", {})
                entry = labels_dict.setdefault(idx, {"fields": {}})

                head = rest_parts[0]

                # labels.<idx>.topic
                if head == "topic" and len(rest_parts) == 1:
                    entry["topic"] = value
                # labels.<idx>.mode
                elif head == "mode" and len(rest_parts) == 1:
                    entry["mode"] = value
                # labels.<idx>.fields.<field_name>
                elif head == "fields" and len(rest_parts) == 2:
                    field_name = rest_parts[1]
                    entry["fields"][field_name] = value

            else:
                pipelines_raw[pipeline_name][subkey] = value

        pipelines: dict[str, PipelineConfig] = {}
        for name, cfg in pipelines_raw.items():
            if "labels" in cfg and isinstance(cfg["labels"], dict):
                label_dict: dict[int, dict[str, Any]] = cfg["labels"]
                cfg["labels"] = [label_dict[i] for i in sorted(label_dict)]
            pipelines[name] = PipelineConfig(**cfg)
        return pipelines

    def load_delay_config(self) -> float:
        """Load subscription delay parameter."""
        if self.has_parameter("subscription_delay_s"):
            return float(self.get_parameter("subscription_delay_s").value)
        return 2.0  # Default delay

    async def init_reduct_bucket(self):
        """Initialize or create ReductStore bucket."""
        settings = BucketSettings(
            quota_type=self.storage_config.quota_type,
            quota_size=self.storage_config.quota_size,
            max_block_size=self.storage_config.max_block_size,
            max_block_records=self.storage_config.max_block_records,
        )
        self.bucket = await self.client.create_bucket(
            self.storage_config.bucket, settings, exist_ok=True
        )

    #
    # Pipeline Writers Initialization
    #
    def init_pipeline_writer(self, pipeline_name: str, cfg: PipelineConfig):
        """
        Create an MCAP/CDR writer for each pipeline.

        For each pipeline, this method creates a timer that fires after max_duration_s
        and a callback to upload the MCAP/CDR data.
        """
        topic_types = dict(self.get_topic_names_and_types())
        all_topics = set(topic_types)

        duration = cfg.split_max_duration_s
        topics = self.resolve_topics(cfg, all_topics)
        if not topics and cfg.include_topics:
            topics = set(cfg.include_topics)

        writer = create_writer(cfg, self.bucket, pipeline_name, self.logger)
        state = PipelineState(
            topics=topics,
            writer=writer,
            downsampler=Downsampler(cfg),
        )

        if cfg.output_format == OutputFormat.MCAP:
            timer = self.create_timer(
                float(duration),
                self.make_timer_callback(pipeline_name, state),
                autostart=False,
            )
            state.timer = timer
            # Add shutdown callback for flushing
            self.context.on_shutdown(writer.flush_on_shutdown)
        else:
            # Add shutdown callback for flushing
            state.timer = None
            self.context.on_shutdown(writer.flush_on_shutdown)

        self.pipeline_states[pipeline_name] = state

        self.log_info(
            lambda: f"Pipeline {pipeline_name} writer "
            f"initialized with config:\n{cfg.format_for_log()}"
        )

    def start_pipeline_timers(self):
        """Start all pipeline timers after topic subscriptions are set up."""
        for pipeline_name, state in self.pipeline_states.items():
            if state.timer:
                state.timer.reset()
                self.log_debug(lambda: f"[{pipeline_name}] Timer started")

    def reset_pipeline_state(
        self,
        pipeline_name: str,
        state: PipelineState,
    ):
        """Finish current Pipeline writer, upload it, and reset writer and buffer."""
        cfg = self.pipeline_configs[pipeline_name]

        new_writer = create_writer(cfg, self.bucket, pipeline_name, self.logger)

        # Update state with new writer
        state.writer = new_writer
        state.current_size = 0
        state.first_timestamp = None
        state.is_uploading = False
        if state.timer:
            state.timer.reset()
        else:
            # Add shutdown callback for flushing (CDR Output)
            self.context.on_shutdown(state.writer.flush_on_shutdown)

        # Clear topics and schemas
        state.schema_by_type.clear()
        state.schemas_by_topic.clear()

        # Recompute topics for this pipeline
        topic_types = dict(self.get_topic_names_and_types())
        all_topics = set(topic_types)
        state.topics = self.resolve_topics(cfg, all_topics)

        # Subscribe to any new topics
        self.setup_topic_subscriptions()

        self.log_debug(
            lambda: f"[{pipeline_name}] Pipeline writer reset - ready for next segment"
        )

    def remove_pipeline(self, pipeline_name: str):
        """Remove pipeline state and stop its timer."""
        state = self.pipeline_states.get(pipeline_name)
        if not state:
            return

        if state.timer:
            self.destroy_timer(state.timer)

        del self.pipeline_states[pipeline_name]
        self.log_info(lambda: f"[{pipeline_name}] Pipeline removed.")

    async def check_diff_pipelines(self, new_configs: dict[str, PipelineConfig]):
        """Check for added, removed, or modified pipelines."""
        current_pipelines = set(self.pipeline_configs.keys())
        new_pipelines = set(new_configs.keys())

        # Removed pipelines
        for pipeline_name in current_pipelines - new_pipelines:
            self.log_info(lambda: f"[{pipeline_name}] Pipeline flushed and removed.")
            await self.pipeline_states[pipeline_name].writer.flush_and_upload_batch()
            self.remove_pipeline(pipeline_name)

        # Added pipelines
        for pipeline_name in new_pipelines - current_pipelines:
            cfg = new_configs[pipeline_name]
            self.pipeline_configs[pipeline_name] = cfg
            self.init_pipeline_writer(pipeline_name, cfg)
        # Ensure new pipeline topics are subscribed
        if new_pipelines - current_pipelines:
            self.setup_topic_subscriptions()

        # Modified pipelines
        for pipeline_name in current_pipelines & new_pipelines:
            if self.pipeline_configs[pipeline_name] != new_configs[pipeline_name]:
                self.log_info(
                    lambda: f"[{pipeline_name}] Pipeline configuration changed. "
                    "Flushing and Resetting pipeline."
                )
                await self.pipeline_states[
                    pipeline_name
                ].writer.flush_and_upload_batch()
                self.pipeline_configs[pipeline_name] = new_configs[pipeline_name]
                state = self.pipeline_states[pipeline_name]
                self.reset_pipeline_state(pipeline_name, state)

    #
    # Topic Subscription
    #
    def resolve_topics(self, cfg: PipelineConfig, all_topics: set[str]) -> set[str]:
        """Resolve topics to subscribe to based on include/exclude patterns."""

        def compile_smart(p: str) -> re.Pattern:
            """Compile a pattern as regex if contains special characters."""
            regex_chars = set(".^$*+?{}[]\\|()")
            if any(c in regex_chars for c in p):
                return re.compile(p)
            return re.compile(f"^{re.escape(p)}$")

        include_patterns = [compile_smart(p) for p in cfg.include_topics]
        exclude_patterns = [compile_smart(p) for p in cfg.exclude_topics]

        return {
            topic
            for topic in all_topics
            if any(r.search(topic) for r in include_patterns)
            and not any(r.search(topic) for r in exclude_patterns)
        }

    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by any pipeline."""
        topic_types = dict(self.get_topic_names_and_types())
        all_topics = set(topic_types)
        topics_to_subscribe: set[str] = set()

        self.log_debug(lambda: f"Discovered {len(all_topics)} topics in ROS2:")
        for topic, types in topic_types.items():
            self.log_debug(lambda: f"  - {topic}: {', '.join(types)}")

        for cfg in self.pipeline_configs.values():
            topics_to_subscribe.update(self.resolve_topics(cfg, all_topics))

        for topic in topics_to_subscribe:
            msg_types = topic_types.get(topic)
            if not msg_types:
                self.log_warn(lambda: f"Skipping '{topic}': No message type found.")
                continue

            msg_type_str = msg_types[0]

            if "/msg/" not in msg_type_str:
                self.log_warn(
                    lambda: f"Skipping '{topic}': "
                    f"Invalid message type format '{msg_type_str}'."
                )
                continue

            self.register_message_schema(topic, msg_type_str)

            if any(sub.topic_name == topic for sub in self.subscribers):
                self.log_debug(lambda: f"Already subscribed to '{topic}'")
                continue

            pkg, msg = msg_type_str.split("/msg/")
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError) as e:
                self.log_warn(
                    lambda e=e: f"Skipping '{topic}': "
                    f"Cannot import '{msg_type_str}' ({e})"
                )
                continue

            sub = self.create_subscription(
                msg_type,
                topic,
                self.make_topic_callback(topic),
                QoSProfile(depth=10),
            )
            self.subscribers.append(sub)
            self.log_info(lambda: f"Subscribed to '{topic}' [{msg_type_str}]")

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema once per message type and associate it with the topic."""
        for state in self.pipeline_states.values():
            if topic_name not in state.topics:
                continue

            # Let the writer handle schema registration internally
            state.writer.register_message_schema(topic_name, msg_type_str)
            state.schemas_by_topic[topic_name] = msg_type_str

    def make_topic_callback(self, topic_name: str):
        """Generate a callback that writes the message to any relevant pipeline."""

        def _topic_callback(message):
            publish_time = self.get_publish_time(message, topic_name)
            self.process_message(topic_name, message, publish_time)

        return _topic_callback

    def get_publish_time(self, message: Any, topic_name: str) -> int:
        """Extract publish time from message in nanoseconds."""
        if hasattr(message, "header") and hasattr(message.header, "stamp"):
            return Time.from_msg(message.header.stamp).nanoseconds
        elif hasattr(message, "stamp"):
            return Time.from_msg(message.stamp).nanoseconds

        if topic_name not in self.warned_topics:
            self.log_warn(
                lambda: f"Message on topic '{topic_name}' "
                "has no timestamp. Using current time."
            )
            self.warned_topics.add(topic_name)

        return self.get_clock().now().nanoseconds

    #
    # Remote Configuration
    #
    async def read_remote_bucket(self) -> str:
        """Read configuration bucket from ReductStore."""
        remote_bucket = await self.client.get_bucket(self.remote_config.bucket)
        entry_name = self.remote_config.entry
        async with remote_bucket.read(entry_name) as record:
            data = await record.read_all()
            yaml_str = data.decode("utf-8")
        return yaml_str

    async def check_remote_updates(self):
        """Periodically check for configuration updates."""
        try:
            yaml_str = await self.read_remote_bucket()
            await self.reload_pipeline_configuration(yaml_str)
            self.log_info(lambda: "Remote configuration fetched and applied.")
        except Exception as exc:
            self.log_warn(lambda exc=exc: f"Failed to fetch configuration: {exc}")

    async def reload_pipeline_configuration(self, yaml_str: str):
        """Reload pipeline configuration."""
        new_config = self.validate_config(yaml_str)
        if new_config is None:
            self.log_warn(
                lambda: "Failed to validate new configuration. "
                "Keeping existing configuration."
            )
        elif new_config == self.pipeline_configs:
            self.log_info(lambda: "No changes in pipeline configuration.")
        else:
            await self.check_diff_pipelines(new_config)
            self.pipeline_configs = new_config
            self.save_backup_yml()
            self.log_info(lambda: "Pipeline configuration updated and backup saved.")

    def validate_config(self, yaml_str: str):
        """Validate fetched config, if not valid use past valid config."""
        try:
            loaded_data = yaml.safe_load(yaml_str)
            pipeline_cfgs = {
                name: PipelineConfig(**cfg)
                for name, cfg in loaded_data.get("pipelines", {}).items()
            }
            self.log_info(lambda: "Pipeline Configuration validated.")
            return pipeline_cfgs
        except Exception as exc:
            self.log_warn(
                lambda exc=exc: f"Configuration validation failed: {exc}. "
                "Using previous valid configuration."
            )
            return None

    def save_backup_yml(self):
        """Save current configuration to backup YAML file in config directory."""
        import os

        backup_data = {
            "storage": self.storage_config.model_dump() if self.storage_config else {},
            "pipelines": {
                name: cfg.model_dump() for name, cfg in self.pipeline_configs.items()
            },
        }
        if self.remote_config is not None:
            backup_data["remote_config"] = self.remote_config.model_dump()

        config_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "config"
        )
        backup_path = os.path.join(config_dir, "config_backup.yml")
        os.makedirs(config_dir, exist_ok=True)
        with open(backup_path, "w") as f:
            yaml.dump(backup_data, f)

    def load_backup_configuration(self):
        """Load backup configuration from config/config_backup.yml if it exists."""
        # Read backup config from config/config_backup.yml
        import os

        backup_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "config",
            "config_backup.yml",
        )
        if os.path.exists(backup_path):
            with open(backup_path, "r") as f:
                backup_data = yaml.safe_load(f)
            if "storage" in backup_data:
                self.storage_config = StorageConfig(**backup_data["storage"])
            if "pipelines" in backup_data:
                self.pipeline_configs = {
                    name: PipelineConfig(**cfg)
                    for name, cfg in backup_data["pipelines"].items()
                }
            if "remote_config" in backup_data:
                self.remote_config = RemoteConfig(**backup_data["remote_config"])
            self.log_info(lambda: f"Loaded backup config from {backup_path}")
        else:
            self.log_warn(lambda: f"No backup config found at {backup_path}")

    #
    # Message Processing
    #
    def process_message(self, topic_name: str, message: Any, publish_time: int):
        """Process message for all pipelines that include the topic."""
        for pipeline_name, state in self.pipeline_states.items():
            if topic_name not in state.topics:
                self.log_debug(
                    lambda: f"Skipping message for pipeline '{pipeline_name}' "
                    f"- topic '{topic_name}' not included."
                )
                continue

            if state.first_timestamp is None:
                state.first_timestamp = publish_time

            if state.downsampler and state.downsampler.downsampling(publish_time):
                continue

            self.log_debug(
                lambda: f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
            )

            schema = state.schemas_by_topic.get(topic_name)
            if not schema:
                self.log_warn(
                    lambda: f"[{pipeline_name}] No schema "
                    f"registered for topic '{topic_name}'"
                )
                continue

            # Write message to pipeline writer
            state.writer.write_message(
                message=message,
                publish_time=publish_time,
                topic=topic_name,
            )

            state.current_size = state.writer.size

    #
    # Timer Callbacks
    #
    def make_timer_callback(self, pipeline_name: str, state: PipelineState):
        """Return a callback that uploads the current pipeline state."""

        def _timer_callback():
            self.upload_pipeline(pipeline_name, state)

        return _timer_callback

    def upload_pipeline(self, pipeline_name: str, state: PipelineState):
        """Trigger upload for the pipeline writer."""
        if not state.writer:
            self.log_warn(
                lambda: f"[{pipeline_name}] No writer available - skipping upload."
            )
            return

        if state.is_uploading:
            self.log_warn(
                lambda: (
                    f"[{pipeline_name}] Upload already in progress"
                    " - skipping upload."
                )
            )
            return

        try:
            self.loop.run_until_complete(state.writer.flush_and_upload_batch())
            if state.timer:
                state.timer.reset()
        except Exception:
            self.log_warn(lambda: f"[{pipeline_name}] Upload failed.")


def main():
    """Start the Recorder node."""
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
