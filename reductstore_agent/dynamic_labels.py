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
    """LabelStateTracker class tracking for dynamic labels."""

    def __init__(self, cfg: PipelineConfig, logger=None):
        """Initialize a LabelStateTracker instance."""
        self._configs: dict[str, LabelTopicConfig] = {
            label_cfg.topic: label_cfg for label_cfg in cfg.labels
        }
        self._values: dict[str, Any] = {}
        self.logger = logger

    def update(self, topic_name, msg):
        """Update label state from a single message incoming message."""
        cfg = self._configs.get(topic_name)
        if cfg is None:
            self.logger.info("Can not read from config. Returning ...")
            return

        if cfg.mode is LabelMode.LAST:
            updater = self._update_last
        elif cfg.mode is LabelMode.FIRST:
            updater = self._update_first
        else:
            updater = self._update_max

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

        try:
            if value > self._values[label_key]:
                self._values[label_key] = value
        except Exception:
            print(Exception)

    def get_labels(self) -> dict[str, str]:
        """Return current labels for writing."""
        return {k: str(v) for k, v in self._value.items()}
