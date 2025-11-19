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

"""Test cdr_output functionality."""

from reductstore_agent.utils import get_or_create_event_loop

from ..utils import fetch_and_count_records, generate_string, publish_and_spin_messages


def test_cdr_output_streams_large_record(
    reduct_client, publisher_node, publisher, cdr_output_recorder
):
    """Test that Recorder streams large messages immediately."""
    large_msg = generate_string(size_kb=150)
    publish_and_spin_messages(
        publisher_node,
        publisher,
        cdr_output_recorder,
        large_msg,
        wait_for_subscription=True,
    )
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    loop = get_or_create_event_loop()
    count = loop.run_until_complete(
        fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME)
    )

    assert len(count) == 1


def test_cdr_output_batch_flushes(
    reduct_client, publisher_node, publisher, cdr_output_recorder
):
    """Test that the Recorder flushes batch."""
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    msg = generate_string(size_kb=90)
    MESSAGE_COUNT = 57
    publish_and_spin_messages(
        publisher_node,
        publisher,
        cdr_output_recorder,
        msg,
        wait_for_subscription=True,
        n_msg=MESSAGE_COUNT,
    )

    loop = get_or_create_event_loop()
    count = loop.run_until_complete(
        fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME)
    )
    assert len(count) == MESSAGE_COUNT


def test_cdr_output_batch_flushed_on_shutdown(
    reduct_client, publisher_node, publisher, cdr_output_recorder
):
    """Test that the Recorder streams and flushes batch on shutdown."""
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"

    msg = generate_string(size_kb=90)
    publish_and_spin_messages(
        publisher_node,
        publisher,
        cdr_output_recorder,
        msg,
        wait_for_subscription=True,
        n_msg=1,
    )
    # Simulate shutdown
    for state in cdr_output_recorder.pipeline_states.values():
        state.writer.flush_on_shutdown()
    loop = get_or_create_event_loop()
    count = loop.run_until_complete(
        fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME)
    )
    assert len(count) == 1
