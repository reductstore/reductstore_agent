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

"""Unit tests for Dynamic Labels."""

from reductstore_agent.dynamic_labels import LabelStateTracker


def test_tracker_last_and_first_modes(mock_label_config):
    """Test LAST mode overwrites and FIRST mode persists the initial value."""
    tracker = LabelStateTracker(mock_label_config)

    # Simulate first message arriving
    tracker.update("/mission_info", {"mid": 100})
    tracker.update("/startup_data", {"volt": 12.5})

    # Simulate second message arriving
    tracker.update("/mission_info", {"mid": 105})
    tracker.update("/startup_data", {"volt": 10.1})

    labels = tracker.get_labels()

    # LAST mode assertion
    assert (
        labels["mission_id"] == "105"
    ), "LAST mode should take the most recent update."

    # FIRST mode assertion
    assert (
        labels["initial_voltage"] == "12.5"
    ), "FIRST mode must keep the initial value."


def test_tracker_max_mode_aggregation(mock_label_config):
    """Test MAX mode correctly finds the highest value across updates."""
    tracker = LabelStateTracker(mock_label_config)

    updates = [50, 45, 75, 60]

    for speed_value in updates:
        tracker.update("/telemetry", {"speed": speed_value})

    assert (
        tracker.get_labels()["max_speed"] == "75"
    ), "MAX mode must select the peak value."


def test_tracker_max_mode_initial_high_value(mock_label_config):
    """Test MAX mode retains the initial value if it's the maximum."""
    tracker = LabelStateTracker(mock_label_config)

    # Sequence: 100 (sets max) -> 50 (ignored)
    tracker.update("/telemetry", {"speed": 100})
    tracker.update("/telemetry", {"speed": 50})

    assert (
        tracker.get_labels()["max_speed"] == "100"
    ), "MAX mode should retain the initial high value."
