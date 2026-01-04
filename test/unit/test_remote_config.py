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

"""Unit tests for Remote Configuration."""

from reductstore_agent.models import PipelineConfig
from reductstore_agent.utils import get_or_create_event_loop
from ..utils import make_pipeline_config


def test_check_diff_pipelines_remove(recorder_with_pipelines):
    """Test removing a pipeline configuration."""
    rec = recorder_with_pipelines
    new_configs = {"pipeline_one": rec.pipeline_configs["pipeline_one"]}
    loop = get_or_create_event_loop()
    loop.run_until_complete(rec.check_diff_pipelines(new_configs))
    assert "pipeline_two" not in rec.pipeline_states
    assert "pipeline_one" in rec.pipeline_states


def test_check_diff_pipelines_add(recorder_with_pipelines):
    """Test adding a new pipeline configuration."""
    rec = recorder_with_pipelines

    new_configs = {
        "pipeline_one": rec.pipeline_configs["pipeline_one"],
        "pipeline_two": rec.pipeline_configs["pipeline_two"],
        "pipeline_three": make_pipeline_config("pipeline_three"),
    }
    loop = get_or_create_event_loop()
    loop.run_until_complete(rec.check_diff_pipelines(new_configs))
    assert "pipeline_one" in rec.pipeline_states
    assert "pipeline_two" in rec.pipeline_states
    assert "pipeline_three" in rec.pipeline_states


def test_check_diff_pipelines_modify(recorder_with_pipelines):
    """Test modifying an existing pipeline configuration."""
    rec = recorder_with_pipelines

    new_configs = {
        "pipeline_one": rec.pipeline_configs["pipeline_one"],
        "pipeline_two": PipelineConfig(
            **{
                "split.max_duration_s": 10,  # Modified parameter
                "include_topics": ["/pipeline_two/topic/data"],
                "filename_mode": "timestamp",
            }
        ),
    }
    assert rec.pipeline_configs["pipeline_two"].split_max_duration_s == 1
    assert rec.pipeline_configs["pipeline_two"].include_topics == ["/topic/two"]
    loop = get_or_create_event_loop()
    loop.run_until_complete(rec.check_diff_pipelines(new_configs))
    assert rec.pipeline_configs["pipeline_two"].split_max_duration_s == 10
    assert rec.pipeline_configs["pipeline_two"].include_topics == [
        "/pipeline_two/topic/data"
    ]


def test_check_diff_pipelines_no_change(recorder_with_pipelines):
    """Test when there are no changes to pipeline configurations."""
    rec = recorder_with_pipelines
    new_configs = rec.pipeline_configs.copy()
    rec.check_diff_pipelines(new_configs)
    assert "pipeline_one" in rec.pipeline_states
    assert "pipeline_two" in rec.pipeline_states
    assert rec.pipeline_configs["pipeline_one"] == new_configs["pipeline_one"]
    assert rec.pipeline_configs["pipeline_two"] == new_configs["pipeline_two"]
