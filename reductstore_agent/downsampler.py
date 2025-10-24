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

"""Downsampler class for the Recorder node."""

from .config_models import DownsamplingMode, PipelineConfig


class Downsampler:
    """Downsampler class for logic and state of a pipeline."""

    def __init__(self, cfg: PipelineConfig):
        """Initialize Downsampler Instance."""
        self.mode = cfg.downsampling_mode

        if self.mode == DownsamplingMode.STRIDE:
            self.stride_n = cfg.stride_n
            self.msg_counter: int = 0
            self.downsampling = self.downsampler_stride

        elif self.mode == DownsamplingMode.MAX_RATE:
            self.max_rate_hz = cfg.max_rate_hz
            self.last_recorded_timestamp: int | None = None
            self.period_ns = 1e9 / self.max_rate_hz
            self.downsampling = self.downsampler_max_rate

        else:
            self.downsampling = self.stub_function

    def downsampler_stride(self, timestamp):
        """Downsample logic for 'stride' mode."""
        if self.stride_n == 0:
            return False

        skip = self.msg_counter % self.stride_n != 0
        self.msg_counter += 1

        return skip

    def downsampler_max_rate(self, timestamp):
        """Downsample logic for 'max_rate' mode."""
        if self.last_recorded_timestamp is None:
            self.last_recorded_timestamp = timestamp
            return False

        skip = (timestamp - self.last_recorded_timestamp) < self.period_ns

        if not skip:
            self.last_recorded_timestamp = timestamp
            return False
        return skip

    def stub_function(self, timestamp):
        """Set downsampling to do nothing."""
        return False
