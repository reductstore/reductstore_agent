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

import pytest

from types import SimpleNamespace
from unittest.mock import MagicMock

from reductstore_agent.recorder import Recorder


@pytest.fixture
def downsampling_func_accessor():
    """
    Returns a callable version of the static down_sampling method
    from the Recorder class.
    """
    return Recorder.down_sampling


def test_downsampling_stride_logic(downsampling_func_accessor):
    """
    Tests the down_sampling function's stride logic (N=5),
    verifying KEEP (False) and SKIP (True) cycles.
    """
    cfg = SimpleNamespace(downsampling_mode='stride', stride_n=5, max_rate_hz=None)
    state = SimpleNamespace(
        msg_counter=0,
        pipeline_name='test',
        get_logger=MagicMock()
    )
    
    down_sampling = downsampling_func_accessor

    assert down_sampling(cfg, state, timestamp=100) is True, "Msg 1 should be skipped"
    assert down_sampling(cfg, state, timestamp=200) is True, "Msg 2 should be skipped"
    assert down_sampling(cfg, state, timestamp=300) is True, "Msg 3 should be skipped"
    assert down_sampling(cfg, state, timestamp=400) is True, "Msg 4 should be skipped"
    assert down_sampling(cfg, state, timestamp=500) is False, "Msg 5 should be kept"
    assert state.msg_counter == 5
    assert down_sampling(cfg, state, timestamp=600) is True, "Msg 6 should be skipped"


def test_downsampling_max_hz_logic(downsampling_func_accessor):
    """Test that the downsampling_mode max_rate logic is working"""

    PERIOD_NS = 100_000_000
    START_TIME = 1_000_000_000
    
    cfg = SimpleNamespace(downsampling_mode='max_rate', max_rate_hz=10.0, stride_n=None)
    state = SimpleNamespace(
        last_recorded_timestamp=None,
        pipeline_name='test',
        get_logger=MagicMock()
    )
    
    down_sampling = downsampling_func_accessor

    assert down_sampling(cfg, state, timestamp=START_TIME) is False, \
        "Msg 1 should be kept."
    assert state.last_recorded_timestamp == START_TIME, \
        "Timestamp should be updated."

    timestamp_too_soon = START_TIME + 50_000_000
    assert down_sampling(cfg, state, timestamp=timestamp_too_soon) is True, \
        "Msg 2 (too soon) should be skipped (True)."
    assert state.last_recorded_timestamp == START_TIME, \
        "Timestamp should NOT be updated."

    timestamp_exact = START_TIME + PERIOD_NS
    assert down_sampling(cfg, state, timestamp=timestamp_exact) is False, \
        "Msg 3 (exact interval) should be kept (False)."
    assert state.last_recorded_timestamp == timestamp_exact, \
        "Timestamp should be updated."

    timestamp_again_too_soon = timestamp_exact + 5_000_000
    assert down_sampling(cfg, state, timestamp=timestamp_again_too_soon) is True, \
        "Msg 4 (too soon from Msg 3) should be skipped."
