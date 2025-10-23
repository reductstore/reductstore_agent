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

"""Test Recorder node downsampling logic."""

from reductstore_agent.config_models import (
    Downsampler,
    DownsamplingMode,
    PipelineConfig,
)


def test_downsampling_stride_logic():
    """Test the Downsampler class configured for 'stride' mode."""
    cfg = PipelineConfig(
        downsampling_mode=DownsamplingMode.STRIDE,
        stride_n=5,
        **{"split.max_duration_s": 1},
    )
    downsampler = Downsampler(cfg)

    assert downsampler.downsampling(timestamp=0) is False, "Msg 0 should be kept"
    assert downsampler.downsampling(timestamp=0) is True, "Msg 1 should be skipped"
    assert downsampler.downsampling(timestamp=0) is True, "Msg 2 should be skipped"
    assert downsampler.downsampling(timestamp=0) is True, "Msg 3 should be skipped"
    assert downsampler.downsampling(timestamp=0) is True, "Msg 4 should be skipped"
    assert downsampler.downsampling(timestamp=0) is False, "Msg 5 should be kept"
    assert downsampler.downsampling(timestamp=0) is True, "Msg 6 should be skipped"


def test_downsampling_max_hz_logic():
    """Test the Downsampler class configured for 'max_rate' mode."""
    PERIOD_NS = 100_000_000
    START_TIME = 1_000_000_000

    cfg = PipelineConfig(
        downsampling_mode=DownsamplingMode.MAX_RATE,
        max_rate_hz=10.0,
        **{"split.max_duration_s": 1},
    )

    downsampler = Downsampler(cfg)

    assert (
        downsampler.downsampling(timestamp=START_TIME) is False
    ), "Msg 1 (first) should be kept."

    timestamp_too_soon = START_TIME + 50_000_000
    assert (
        downsampler.downsampling(timestamp=timestamp_too_soon) is True
    ), "Msg 2 (too soon) should be skipped."

    timestamp_exact = START_TIME + PERIOD_NS
    assert (
        downsampler.downsampling(timestamp=timestamp_exact) is False
    ), "Msg 3 (exact interval) should be kept."

    timestamp_too_soon = timestamp_exact + 5_000_000
    assert (
        downsampler.downsampling(timestamp=timestamp_too_soon) is True
    ), "Msg 4 (too soon from Msg 3) should be skipped."
