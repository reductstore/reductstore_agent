import pytest
from rclpy.parameter import Parameter

from ros2_reduct_agent.recorder import Recorder


def storage_params():
    return {
        "url": "http://localhost:8383",
        "api_token": "dummy_token",
        "bucket": "test_bucket",
    }


def pipeline_params():
    return [
        Parameter(
            "pipelines.test.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.test.split.max_duration_s",
            Parameter.Type.INTEGER,
            1,
        ),
        Parameter(
            "pipelines.test.split.max_size_bytes",
            Parameter.Type.INTEGER,
            1,
        ),
    ]


def as_overrides(storage_dict):
    """Convert storage parameters and combine with pipeline parameters."""
    return [
        Parameter(f"storage.{k}", Parameter.Type.STRING, v)
        for k, v in storage_dict.items()
    ] + pipeline_params()


def test_recorder_valid_params():
    """Test that the Recorder node can be created with valid parameters."""
    node = Recorder(parameter_overrides=as_overrides(storage_params()))
    assert node.get_name() == "recorder"
    node.destroy_node()


@pytest.mark.parametrize("missing_key", ["url", "api_token", "bucket"])
def test_recorder_missing_param(missing_key):
    """Test that the Recorder node raises an error if a required parameter is missing."""
    params = storage_params()
    params.pop(missing_key)
    with pytest.raises(
        SystemExit, match=rf"Missing parameter: 'storage\.{missing_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize("empty_key", ["url", "api_token", "bucket"])
def test_recorder_empty_value(empty_key):
    """Test that the Recorder node raises an error if a required parameter is empty."""
    params = storage_params()
    params[empty_key] = ""
    with pytest.raises(
        SystemExit, match=f"Empty value for parameter: 'storage\.{empty_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))
