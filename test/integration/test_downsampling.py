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

"""Integration test for downsampling logic."""

from reductstore_agent.utils import get_or_create_event_loop

from ..utils import fetch_and_count_records, generate_string, publish_and_spin_messages


def test_stride_downsampling(reduct_client, publisher_node, publisher, stride_recorder):
    """Test that the downsampling method 'stride' works."""
    MESSAGE_COUNT = 50
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"
    EXPECTED_COUNT = MESSAGE_COUNT / 5  # default stride value
    msg = generate_string(size_kb=90)

    publish_and_spin_messages(
        publisher_node,
        publisher,
        stride_recorder,
        msg,
        wait_for_subscription=True,
        n_msg=MESSAGE_COUNT,
    )

    loop = get_or_create_event_loop()
    count = loop.run_until_complete(
        fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME)
    )

    assert len(count) == EXPECTED_COUNT


def test_max_rate_downsampling(
    reduct_client, publisher_node, publisher, max_rate_recorder
):
    """Test that 'max_rate' downsampling mode works."""
    """Test that the downsampling method 'max_rate' works."""
    MESSAGE_COUNT = 50
    ENTRY_NAME = "test"
    BUCKET_NAME = "test_bucket"
    EXPECTED_COUNT = 10

    msg = generate_string(size_kb=90)

    # Publishing 50 messages leads to 100Hz of the recorder,
    # 10 Hz is default of max_stride
    publish_and_spin_messages(
        publisher_node,
        publisher,
        max_rate_recorder,
        msg,
        wait_for_subscription=True,
        n_msg=MESSAGE_COUNT,
    )

    loop = get_or_create_event_loop()
    records = loop.run_until_complete(
        fetch_and_count_records(reduct_client, BUCKET_NAME, ENTRY_NAME)
    )

    assert len(records) == EXPECTED_COUNT
