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

"""Label state tracker for dynamic labels."""

from typing import Any

from .models import LabelMode, LabelTopicConfig, PipelineConfig
from .utils import extract_field


class LabelStateTracker:
    """Class for tracking dynamic label state."""

    def __init__(self, cfg: PipelineConfig, logger=None):
        """Initialize a LabelStateTracker instance."""
        self._configs: dict[str, LabelTopicConfig] = {
            label_cfg.topic: label_cfg for label_cfg in cfg.labels
        }
        self.logger = logger
        self._updaters: dict[str, callable] = {}

        for topic, label_cfg in self._configs.items():
            if label_cfg.mode is LabelMode.LAST:
                self._updaters[topic] = self._update_last
            elif label_cfg.mode is LabelMode.FIRST:
                self._updaters[topic] = self._update_first
            else:
                self._updaters[topic] = self._update_max

        self._values: dict[str, Any] = {}

    def update(self, topic_name, msg):
        """Update label state from a single incoming message."""
        cfg = self._configs.get(topic_name)
        if cfg is None:
            if self.logger:
                self.logger.info(
                    "Cannot read config for topic " f"'{topic_name}'. Returning ..."
                )
            return
        
        updater = self._updaters[topic_name]

        for label_name, field_path in cfg.fields.items():
            value = extract_field(msg, field_path)
            updater(label_name, value)

    def _update_last(self, label_key: str, value: Any):
        """Use the most recent message (default)."""
        self._values[label_key] = value

    def _update_first(self, label_key: str, value: Any):
        """Use the first message of the current file."""
        if label_key not in self._values:
            self._values[label_key] = value

    def _update_max(self, label_key: str, value: Any):
        """Use the maximum value across all messages in the file."""
        if label_key not in self._values:
            self._values[label_key] = value
            return

        if value > self._values[label_key]:
            self._values[label_key] = value

    def get_labels(self) -> dict[str, str]:
        """Return current labels for writing."""
        return {k: str(v) for k, v in self._values.items()}


class NullLabelStateTracker(LabelStateTracker):
    """Null object for LabelStateTracker."""

    def __init__(self):
        """Initialize the null object without configuration."""
        pass

    def update(self, topic_name, msg):
        """Do nothing on update, as there is no state to track."""
        pass

    def get_labels(self) -> dict[str, str]:
        """Return the default/empty state."""
        return {}
