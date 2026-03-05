# Copyright 2026 ReductSoftware UG
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

"""Pipeline lifecycle and topic subscription management."""

import importlib
import re
from typing import Any

from rclpy.qos import QoSProfile

from .downsampler import Downsampler
from .models import OutputFormat, PipelineConfig
from .state import PipelineState
from .writer import create_writer


class PipelineManager:
    """Manage pipeline states, subscriptions, and runtime updates."""

    def __init__(self, node):
        """Initialize manager with Recorder node dependency."""
        self.node = node

    def init_pipeline_writer(self, pipeline_name: str, cfg: PipelineConfig):
        """Create and initialize writer/state for a pipeline."""
        topic_types = dict(self.node.get_topic_names_and_types())
        all_topics = set(topic_types)

        duration = cfg.split_max_duration_s
        topics = self.resolve_topics(cfg, all_topics)
        if not topics and cfg.include_topics:
            topics = set(cfg.include_topics)

        writer = create_writer(cfg, self.node.bucket, pipeline_name, self.node.logger)
        state = PipelineState(
            topics=topics,
            writer=writer,
            downsampler=Downsampler(cfg),
        )

        if cfg.output_format == OutputFormat.MCAP:
            timer = self.node.create_timer(
                float(duration),
                self.node.make_timer_callback(pipeline_name, state),
                autostart=False,
            )
            state.timer = timer
            self.node.context.on_shutdown(writer.flush_on_shutdown)
        else:
            state.timer = None
            self.node.context.on_shutdown(writer.flush_on_shutdown)

        self.node.pipeline_states[pipeline_name] = state
        self.node.log_info(
            lambda: f"Pipeline {pipeline_name} writer "
            f"initialized with config:\n{cfg.format_for_log()}"
        )

    def start_pipeline_timers(self):
        """Start timers for all active pipelines."""
        for pipeline_name, state in self.node.pipeline_states.items():
            if state.timer:
                state.timer.reset()
                self.node.log_debug(lambda: f"[{pipeline_name}] Timer started")

    def reset_pipeline_state(self, pipeline_name: str, state: PipelineState):
        """Reset writer and state for a pipeline segment."""
        cfg = self.node.pipeline_configs[pipeline_name]
        new_writer = create_writer(
            cfg, self.node.bucket, pipeline_name, self.node.logger
        )

        state.writer = new_writer
        state.current_size = 0
        state.first_timestamp = None
        state.is_uploading = False
        if state.timer:
            state.timer.reset()
        else:
            self.node.context.on_shutdown(state.writer.flush_on_shutdown)

        state.schema_by_type.clear()
        state.schemas_by_topic.clear()

        topic_types = dict(self.node.get_topic_names_and_types())
        all_topics = set(topic_types)
        state.topics = self.resolve_topics(cfg, all_topics)

        self.setup_topic_subscriptions()
        self.node.log_debug(
            lambda: f"[{pipeline_name}] Pipeline writer reset - ready for next segment"
        )

    def remove_pipeline(self, pipeline_name: str):
        """Remove pipeline state and stop associated timer."""
        state = self.node.pipeline_states.get(pipeline_name)
        if not state:
            return

        if state.timer:
            self.node.destroy_timer(state.timer)

        del self.node.pipeline_states[pipeline_name]
        self.node.log_info(lambda: f"[{pipeline_name}] Pipeline removed.")

    async def check_diff_pipelines(self, new_configs: dict[str, PipelineConfig]):
        """Apply diff between current and new pipeline configurations."""
        current_pipelines = set(self.node.pipeline_configs.keys())
        new_pipelines = set(new_configs.keys())

        for pipeline_name in current_pipelines - new_pipelines:
            self.node.log_info(
                lambda: f"[{pipeline_name}] " "Pipeline flushed and removed."
            )
            await self.node.pipeline_states[
                pipeline_name
            ].writer.flush_and_upload_batch()
            self.remove_pipeline(pipeline_name)

        for pipeline_name in new_pipelines - current_pipelines:
            cfg = new_configs[pipeline_name]
            self.node.pipeline_configs[pipeline_name] = cfg
            self.init_pipeline_writer(pipeline_name, cfg)

        for pipeline_name in current_pipelines & new_pipelines:
            if self.node.pipeline_configs[pipeline_name] != new_configs[pipeline_name]:
                self.node.log_info(
                    lambda: f"[{pipeline_name}] Pipeline configuration changed. "
                    "Flushing and reinitializing pipeline."
                )
                await self.node.pipeline_states[
                    pipeline_name
                ].writer.flush_and_upload_batch()
                self.remove_pipeline(pipeline_name)
                self.node.pipeline_configs[pipeline_name] = new_configs[pipeline_name]
                self.init_pipeline_writer(pipeline_name, new_configs[pipeline_name])

        self.setup_topic_subscriptions()

    def resolve_topics(self, cfg: PipelineConfig, all_topics: set[str]) -> set[str]:
        """Resolve topics using include/exclude patterns."""

        def compile_smart(pattern: str) -> re.Pattern:
            regex_chars = set(".^$*+?{}[]\\|()")
            if any(c in regex_chars for c in pattern):
                return re.compile(pattern)
            return re.compile(f"^{re.escape(pattern)}$")

        include_patterns = [compile_smart(p) for p in cfg.include_topics]
        exclude_patterns = [compile_smart(p) for p in cfg.exclude_topics]

        return {
            topic
            for topic in all_topics
            if any(r.search(topic) for r in include_patterns)
            and not any(r.search(topic) for r in exclude_patterns)
        }

    def setup_topic_subscriptions(self):
        """Subscribe to all topics referenced by configured pipelines."""
        topic_types = dict(self.node.get_topic_names_and_types())
        all_topics = set(topic_types)
        topics_to_subscribe: set[str] = set()

        self.node.log_debug(lambda: f"Discovered {len(all_topics)} topics in ROS2:")
        for topic, types in topic_types.items():
            self.node.log_debug(lambda: f"  - {topic}: {', '.join(types)}")

        for cfg in self.node.pipeline_configs.values():
            topics_to_subscribe.update(self.resolve_topics(cfg, all_topics))

        for topic in topics_to_subscribe:
            msg_types = topic_types.get(topic)
            if not msg_types:
                self.node.log_warn(
                    lambda: f"Skipping '{topic}': No message type found."
                )
                continue

            msg_type_str = msg_types[0]

            if "/msg/" not in msg_type_str:
                self.node.log_warn(
                    lambda: f"Skipping '{topic}': "
                    f"Invalid message type format '{msg_type_str}'."
                )
                continue

            self.register_message_schema(topic, msg_type_str)

            if any(sub.topic_name == topic for sub in self.node.subscribers):
                self.node.log_debug(lambda: f"Already subscribed to '{topic}'")
                continue

            pkg, msg = msg_type_str.split("/msg/")
            try:
                module = importlib.import_module(f"{pkg}.msg")
                msg_type = getattr(module, msg)
            except (ModuleNotFoundError, AttributeError) as exc:
                msg = f"Skipping '{topic}': Cannot import '{msg_type_str}' ({exc})"
                self.node.log_warn(lambda: msg)
                continue

            sub = self.node.create_subscription(
                msg_type,
                topic,
                self.node.make_topic_callback(topic),
                QoSProfile(depth=10),
            )
            self.node.subscribers.append(sub)
            self.node.log_info(lambda: f"Subscribed to '{topic}' [{msg_type_str}]")

    def register_message_schema(self, topic_name: str, msg_type_str: str):
        """Register schema for all pipelines matching topic."""
        for state in self.node.pipeline_states.values():
            if topic_name not in state.topics:
                continue

            state.writer.register_message_schema(topic_name, msg_type_str)
            state.schemas_by_topic[topic_name] = msg_type_str

    def process_message(self, topic_name: str, message: Any, publish_time: int):
        """Write a message to each matching pipeline writer."""
        for pipeline_name, state in self.node.pipeline_states.items():
            if topic_name not in state.topics:
                self.node.log_debug(
                    lambda: (
                        f"Skipping message for pipeline '{pipeline_name}' "
                        f"- topic '{topic_name}' not included."
                    )
                )
                continue

            if state.first_timestamp is None:
                state.first_timestamp = publish_time

            if state.downsampler and state.downsampler.downsampling(publish_time):
                continue

            self.node.log_debug(
                lambda: (
                    f"Writing message to pipeline '{pipeline_name}' [{topic_name}]"
                )
            )

            schema = state.schemas_by_topic.get(topic_name)
            if not schema:
                self.node.log_warn(
                    lambda: (
                        f"[{pipeline_name}] No schema registered for"
                        f" topic '{topic_name}'"
                    )
                )
                continue

            state.writer.write_message(
                message=message,
                publish_time=publish_time,
                topic=topic_name,
            )

            state.current_size = state.writer.size

    def upload_pipeline(self, pipeline_name: str, state: PipelineState):
        """Trigger writer upload for one pipeline."""
        if not state.writer:
            self.node.log_warn(
                lambda: f"[{pipeline_name}] No writer available - skipping upload."
            )
            return

        if state.is_uploading:
            self.node.log_warn(
                lambda: (
                    f"[{pipeline_name}] Upload already in progress - skipping upload."
                )
            )
            return

        try:
            self.node.loop.run_until_complete(state.writer.flush_and_upload_batch())
            if state.timer:
                state.timer.reset()
        except Exception:
            self.node.log_warn(lambda: f"[{pipeline_name}] Upload failed.")
