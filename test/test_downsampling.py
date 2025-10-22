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
import io

from reduct.client import Client

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from types import SimpleNamespace
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory
from rclpy.publisher import Publisher
from std_msgs.msg import String
import time

from reductstore_agent.recorder import (
    Recorder,
    create_stride_downsampler,
    create_max_rate_downsampler
)
from test_recorder_params import (
    as_overrides,
    storage_params,
    pipeline_params,
    downsampling_params_max_rate,
    downsampling_params_stride
)
from test_storage import get_or_create_event_loop

@pytest.fixture
def downsampling_func_accessor():
    """Get the callable static down_sampling method."""
    return Recorder.down_sampling


def test_downsampling_stride_logic():
    """Test that 'stride' logic works."""
    cfg = SimpleNamespace(stride_n=5)
    state = SimpleNamespace(msg_counter=0)
    stride_downsampler = create_stride_downsampler(cfg)

    assert stride_downsampler(state, timestamp=0) is False, "Msg 0 should be kept"
    state.msg_counter += 1
    assert stride_downsampler(state, timestamp=0) is True, "Msg 1 should be skipped"
    state.msg_counter += 1
    assert stride_downsampler(state, timestamp=0) is True, "Msg 2 should be skipped"
    state.msg_counter += 1
    assert stride_downsampler(state, timestamp=0) is True, "Msg 3 should be skipped"
    state.msg_counter += 1
    assert stride_downsampler(state, timestamp=0) is True, "Msg 4 should be skipped"
    state.msg_counter += 1
    assert stride_downsampler(state, timestamp=0) is False, "Msg 5 should be kept"


def test_downsampling_max_hz_logic():
    """Test that 'max_rate' logic is working."""
    PERIOD_NS = 100_000_000
    START_TIME = 1_000_000_000

    cfg = SimpleNamespace(max_rate_hz=10.0)
    state = SimpleNamespace(last_recorded_timestamp=None)
    max_rate_downsampler = create_max_rate_downsampler(cfg)

    assert (
        max_rate_downsampler(state, timestamp=START_TIME) is False
    ), "Msg 1 should be kept."

    state.last_recorded_timestamp = START_TIME

    timestamp_too_soon = START_TIME + 50_000_000
    assert (
        max_rate_downsampler(state, timestamp=timestamp_too_soon) is True
    ), "Msg 2 (too soon) should be skipped."
    timestamp_exact = START_TIME + PERIOD_NS
    assert (
        max_rate_downsampler(state, timestamp=timestamp_exact) is False
    ), "Msg 3 (exact interval) should be kept."
    state.last_recorded_timestamp = timestamp_exact

    timestamp_again_too_soon = timestamp_exact + 5_000_000
    assert (
        max_rate_downsampler(state, timestamp=timestamp_again_too_soon) is True
    ), "Msg 4 (too soon from Msg 3) should be skipped."




@pytest.fixture
def stride_recorder():
    """Create a Recorder instance with 'stride' mode."""
    node = Recorder(
        parameter_overrides=as_overrides(
            storage_params(),
            pipeline_params(),
            # downsampling_params_stride()
        )
    )
    yield node
    node.destroy_node()

def test_downsampling_integration_stride(
    reduct_client: Client, publisher_node: rclpy.node, publisher: Publisher, stride_recorder: Recorder
):
    TOTAL_MESSAGES = 50
    EXPECTED_RECORDED_MESSAGES = 10

    publish_and_spin(
        publisher_node,
        publisher,
        stride_recorder,
        n_msgs=TOTAL_MESSAGES,
    )
    async def fetch():
        bucket = await reduct_client.get_bucket("test_bucket")
        count = 0
        async for record in bucket.query("test"):
            reader = make_reader(
                io.BytesIO(await record.read_all()),
                decoder_factories=[DecoderFactory()]
            )
            for _ in reader.iter_decoded_messages():
                count += 1
        return count

    recorded_count = get_or_create_event_loop().run_until_complete(fetch())

    assert recorded_count == EXPECTED_RECORDED_MESSAGES, (
        f"Expected {EXPECTED_RECORDED_MESSAGES} messages after downsampling, "
        f"found {recorded_count}."


    )
