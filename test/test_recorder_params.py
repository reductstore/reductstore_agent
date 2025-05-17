import re

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
            1_000_000,
        ),
    ]


def as_overrides(storage_dict, pipeline_params=None):
    """Convert storage parameters and combine with pipeline parameters."""
    overrides = []
    for key, value in storage_dict.items():
        overrides.append(
            Parameter(
                f"storage.{key}",
                Parameter.Type.STRING,
                value,
            )
        )
    if pipeline_params:
        for param in pipeline_params:
            overrides.append(param)
    return overrides


def test_recorder_valid_params():
    """Test that the Recorder node can be created with valid parameters."""
    node = Recorder(parameter_overrides=as_overrides(storage_params()))
    assert node.get_name() == "recorder"
    node.destroy_node()


@pytest.mark.parametrize("missing_key", ["url", "api_token", "bucket"])
def test_recorder_missing_storage_param(missing_key):
    """Test that the Recorder node raises an error if a required parameter is missing."""
    params = storage_params()
    params.pop(missing_key)
    with pytest.raises(
        SystemExit, match=rf"Missing parameter: 'storage\.{missing_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize("empty_key", ["url", "api_token", "bucket"])
def test_recorder_empty_storage_value(empty_key):
    """Test that the Recorder node raises an error if a required parameter is empty."""
    params = storage_params()
    params[empty_key] = ""
    with pytest.raises(
        SystemExit, match=f"Empty value for parameter: 'storage\.{empty_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize(
    "param_name, invalid_value, err_msg",
    [
        (
            "pipelines.test.split.max_duration_s",
            0,
            "pipelines.test.split.max_duration_s' should be an int between 1 and 3600s. Got: 0",
        ),
        (
            "pipelines.test.split.max_duration_s",
            3601,
            "pipelines.test.split.max_duration_s' should be an int between 1 and 3600s. Got: 3601",
        ),
        (
            "pipelines.test.split.max_size_bytes",
            999,
            "pipelines.test.split.max_size_bytes' should be an int between 1KB and 1GB. Got: 999",
        ),
        (
            "pipelines.test.split.max_size_bytes",
            1000000001,
            "pipelines.test.split.max_size_bytes' should be an int between 1KB and 1GB. Got: 1000000001",
        ),
        (
            "pipelines.test.include_topics",
            ["/valid_topic", "invalid_topic"],
            "'pipelines.test.include_topics' should be a list of ROS topic names starting with '/'.",
        ),
    ],
)
def test_recorder_invalid_pipeline_param(param_name, invalid_value, err_msg):
    """Test that the Recorder node raises an error if a pipeline parameter is invalid."""
    storage_params_dict = storage_params()
    pipeline_params_list = []
    for param in pipeline_params():
        if param.name == param_name:
            pipeline_params_list.append(
                Parameter(param.name, param.type_, invalid_value)
            )
        else:
            pipeline_params_list.append(param)

    with pytest.raises(SystemExit, match=rf"{err_msg}"):
        Recorder(
            parameter_overrides=as_overrides(storage_params_dict, pipeline_params_list)
        )


def test_recorder_invalid_pipeline_param_name():
    """Test that the Recorder node raises an error for pipeline parameters with invalid names (less than 3 parts)."""
    storage_dict = storage_params()
    invalid_pipeline_param = Parameter(
        "pipelines.invalid", Parameter.Type.STRING, "something"
    )

    with pytest.raises(
        SystemExit,
        match=re.escape(
            "Invalid pipeline parameter name: 'pipelines.invalid'. Expected 'pipelines.<pipeline_name>.<subkey>'"
        ),
    ):
        Recorder(
            parameter_overrides=as_overrides(storage_dict, [invalid_pipeline_param])
        )
