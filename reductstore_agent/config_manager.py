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

"""Configuration loading and remote configuration management."""

import os
from collections import defaultdict
from typing import Any

import yaml

from .models import PipelineConfig, RemoteConfig, StorageConfig


class ConfigManager:
    """Manage local and remote configuration lifecycle."""

    def __init__(self, node):
        """Initialize manager with Recorder node dependency."""
        self.node = node

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
            if not self.node.has_parameter(param):
                raise ValueError(f"Missing parameter: '{param}'")
            params[key] = self.node.get_parameter(param).value

        for key in optional_keys:
            param = f"storage.{key}"
            if self.node.has_parameter(param):
                params[key] = self.node.get_parameter(param).value

        return StorageConfig(**params)

    def load_remote_config(self):
        """Parse and validate remote configuration parameters."""
        required_keys = ["url", "api_token", "bucket", "entry"]
        optional_keys = ["pull_frequency_s"]

        params = {}
        for key in required_keys:
            param = f"remote.{key}"
            if not self.node.has_parameter(param):
                self.node.log_info(lambda: "No remote configuration parameters found.")
                return None
            params[key] = self.node.get_parameter(param).value

        for key in optional_keys:
            param = f"remote.{key}"
            if self.node.has_parameter(param):
                params[key] = self.node.get_parameter(param).value

        return RemoteConfig(**params)

    def load_pipeline_config(self) -> dict[str, PipelineConfig]:
        """Parse and validate pipeline parameters."""
        pipelines_raw: dict[str, dict[str, Any]] = defaultdict(dict)
        for param in self.node.get_parameters_by_prefix("pipelines").values():
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
                if head == "topic" and len(rest_parts) == 1:
                    entry["topic"] = value
                elif head == "mode" and len(rest_parts) == 1:
                    entry["mode"] = value
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

    async def read_remote_bucket(self) -> str:
        """Read YAML configuration payload from remote ReductStore bucket."""
        remote_bucket = await self.node.client.get_bucket(
            self.node.remote_config.bucket
        )
        entry_name = self.node.remote_config.entry
        async with remote_bucket.read(entry_name) as record:
            data = await record.read_all()
            yaml_str = data.decode("utf-8")
        return yaml_str

    async def check_remote_updates(self):
        """Fetch and apply remote configuration updates."""
        try:
            yaml_str = await self.read_remote_bucket()
            await self.reload_pipeline_configuration(yaml_str)
            self.node.log_info(lambda: "Remote configuration fetched and applied.")
        except Exception as exc:
            self.node.log_warn(lambda exc=exc: f"Failed to fetch configuration: {exc}")

    async def reload_pipeline_configuration(self, yaml_str: str):
        """Reload and apply pipeline configuration if it changed."""
        new_config = self.validate_config(yaml_str)
        if new_config is None:
            self.node.log_warn(
                lambda: "Failed to validate new configuration. "
                "Keeping existing configuration."
            )
        elif new_config == self.node.pipeline_configs:
            self.node.log_info(lambda: "No changes in pipeline configuration.")
        else:
            await self.node.check_diff_pipelines(new_config)
            self.node.pipeline_configs = new_config
            self.save_backup_yml()
            self.node.log_info(
                lambda: "Pipeline configuration updated and backup saved."
            )

    def validate_config(self, yaml_str: str):
        """Validate fetched config, if not valid use past valid config."""
        try:
            loaded_data = yaml.safe_load(yaml_str)
            pipeline_cfgs = {
                name: PipelineConfig(**cfg)
                for name, cfg in loaded_data.get("pipelines", {}).items()
            }
            self.node.log_info(lambda: "Pipeline Configuration validated.")
            return pipeline_cfgs
        except Exception as exc:
            self.node.log_warn(
                lambda exc=exc: f"Configuration validation failed: {exc}. "
                "Using previous valid configuration."
            )
            return None

    def save_backup_yml(self):
        """Save current configuration to config/config_backup.yml."""

        def enum_to_str(obj):
            if hasattr(obj, "__class__") and hasattr(obj, "name"):
                return str(obj.name)
            return obj

        def clean_dict(data):
            if isinstance(data, dict):
                return {k: clean_dict(v) for k, v in data.items()}
            if isinstance(data, list):
                return [clean_dict(v) for v in data]
            return enum_to_str(data)

        backup_data = {
            "storage": (
                clean_dict(self.node.storage_config.model_dump())
                if self.node.storage_config
                else {}
            ),
            "pipelines": {
                name: clean_dict(cfg.model_dump())
                for name, cfg in self.node.pipeline_configs.items()
            },
        }
        if self.node.remote_config is not None:
            backup_data["remote_config"] = clean_dict(
                self.node.remote_config.model_dump()
            )

        config_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "config"
        )
        backup_path = os.path.join(config_dir, "config_backup.yml")
        try:
            os.makedirs(config_dir, exist_ok=True)
            with open(backup_path, "w") as file:
                yaml.safe_dump(backup_data, file, default_flow_style=False)
        except Exception as exc:
            self.node.log_warn(
                lambda exc=exc: f"Failed to save backup configuration: {exc}"
            )

    def load_backup_configuration(self):
        """Load backup configuration from config/config_backup.yml."""
        backup_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "config",
            "config_backup.yml",
        )
        if not os.path.exists(backup_path):
            self.node.log_warn(lambda: f"No backup config found at {backup_path}")
            return

        with open(backup_path, "r") as file:
            backup_data = yaml.safe_load(file)

        if "storage" in backup_data:
            self.node.storage_config = StorageConfig(**backup_data["storage"])
        if "pipelines" in backup_data:
            self.node.pipeline_configs = {
                name: PipelineConfig(**cfg)
                for name, cfg in backup_data["pipelines"].items()
            }
        if "remote_config" in backup_data:
            self.node.remote_config = RemoteConfig(**backup_data["remote_config"])
        self.node.log_info(lambda: f"Loaded backup config from {backup_path}")
