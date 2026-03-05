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

from typing import Any

import rclpy
from rclpy.impl.logging_severity import LoggingSeverity
from rclpy.node import Node
from rclpy.subscription import Subscription
from rclpy.time import Time
from reduct import BucketSettings, Client

from .config_manager import ConfigManager
from .models import PipelineConfig, StorageConfig
from .pipeline_manager import PipelineManager
from .state import PipelineState
from .utils import get_or_create_event_loop


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
        self.config_manager = ConfigManager(self)
        self.pipeline_manager = PipelineManager(self)

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
                if self.pipeline_configs:
                    for name, cfg in self.pipeline_configs.items():
                        self.init_pipeline_writer(name, cfg)

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
        return self.config_manager.load_storage_config()

    def load_remote_config(self):
        """Parse and validate configuration params."""
        return self.config_manager.load_remote_config()

    def load_pipeline_config(self) -> dict[str, PipelineConfig]:
        """Parse and validate pipeline parameters."""
        return self.config_manager.load_pipeline_config()

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
        """Create and initialize writer/state for a pipeline."""
        self.pipeline_manager.init_pipeline_writer(pipeline_name, cfg)

    def start_pipeline_timers(self):
        """Start all pipeline timers after topic subscriptions are set up."""
        self.pipeline_manager.start_pipeline_timers()

    def reset_pipeline_state(
        self,
        pipeline_name: str,
        state: PipelineState,
    ):
        """Reset writer and state for a pipeline segment."""
        self.pipeline_manager.reset_pipeline_state(pipeline_name, state)

    def remove_pipeline(self, pipeline_name: str):
        """Remove pipeline state and stop its timer."""
        self.pipeline_manager.remove_pipeline(pipeline_name)

    async def check_diff_pipelines(self, new_configs: dict[str, PipelineConfig]):
        """Check for added, removed, or modified pipelines."""
        await self.pipeline_manager.check_diff_pipelines(new_configs)

    #
    # Topic Subscription
    #

    def resolve_topics(self, cfg: PipelineConfig, all_topics: set[str]) -> set[str]:
        """Resolve topics to subscribe to based on include/exclude patterns."""
        return self.pipeline_manager.resolve_topics(cfg, all_topics)

    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by any pipeline."""
        self.pipeline_manager.setup_topic_subscriptions()

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema once per message type and associate it with the topic."""
        self.pipeline_manager.register_message_schema(topic_name, msg_type_str)

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
        return await self.config_manager.read_remote_bucket()

    async def check_remote_updates(self):
        """Periodically check for configuration updates."""
        await self.config_manager.check_remote_updates()

    async def reload_pipeline_configuration(self, yaml_str: str):
        """Reload pipeline configuration."""
        await self.config_manager.reload_pipeline_configuration(yaml_str)

    def validate_config(self, yaml_str: str):
        """Validate fetched config, if not valid use past valid config."""
        return self.config_manager.validate_config(yaml_str)

    def save_backup_yml(self):
        """Save current configuration to backup YAML file in config directory."""
        self.config_manager.save_backup_yml()

    def load_backup_configuration(self):
        """Load backup configuration from config/config_backup.yml if it exists."""
        self.config_manager.load_backup_configuration()

    #
    # Message Processing
    #
    def process_message(self, topic_name: str, message: Any, publish_time: int):
        """Process message for all pipelines that include the topic."""
        self.pipeline_manager.process_message(topic_name, message, publish_time)

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
        self.pipeline_manager.upload_pipeline(pipeline_name, state)


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
