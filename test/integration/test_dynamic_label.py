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

"""Integration test for dynamic labels."""

from reductstore_agent.utils import get_or_create_event_loop

from ..utils import fetch_and_count_records, publish_and_spin_messages_multi


def test_dynamic_label(
    reduct_client, publisher_node, label_publishers, dynamic_label_recorder
):
    """Test that the recorder correctly uses all modes."""
    BUCKET_NAME = "test_bucket"
    ENTRY_NAME = "test"

    publish_and_spin_messages_multi(
        publisher_node, label_publishers, dynamic_label_recorder, True, 5
    )

    loop = get_or_create_event_loop()
    records = loop.run_until_complete(
        fetch_and_count_records(
            reduct_client,
            BUCKET_NAME,
            ENTRY_NAME,
        )
    )
    assert len(records) == 3
    last_record = records[-1]
    labels = last_record.labels

    assert labels.get("initial_voltage") == "12.0"  # FIRST
    assert labels.get("max_speed") == "90"  # MAX
    assert labels.get("mission_id") == "Run_4"  # LAST
