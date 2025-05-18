import re

import pytest
from rclpy.parameter import Parameter

from ros2_reduct_agent.recorder import Recorder


def storage_params():
    return {
        "url": "http://localhost:8383",
        "api_token": "test_token",
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
        Parameter(
            "pipelines.test.filename_mode",
            Parameter.Type.STRING,
            "counter",
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


def test_recorder_valid_storage_params():
    """Test that the Recorder node can be created with valid parameters."""
    node = Recorder(parameter_overrides=as_overrides(storage_params()))
    assert node.get_name() == "recorder"
    node.destroy_node()


def test_recorder_valid_pipeline_params():
    """Test that the Recorder node can be created with valid pipeline parameters."""
    node = Recorder(
        parameter_overrides=as_overrides(storage_params(), pipeline_params())
    )
    assert node.get_name() == "recorder"
    node.destroy_node()


@pytest.mark.parametrize("missing_key", ["url", "api_token", "bucket"])
def test_recorder_missing_storage_param(missing_key):
    """Test that the Recorder node raises an error if a required parameter is missing."""
    params = storage_params()
    params.pop(missing_key)
    with pytest.raises(
        ValueError, match=rf"Missing parameter: 'storage\.{missing_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize("empty_key", ["url", "api_token", "bucket"])
def test_recorder_empty_storage_value(empty_key):
    """Test that the Recorder node raises an error if a required parameter is empty."""
    params = storage_params()
    params[empty_key] = ""
    with pytest.raises(ValueError, match=f"'{empty_key}' must not be empty"):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize(
    "param_name, invalid_value, err_msg",
    [
        (
            "pipelines.test.split.max_duration_s",
            0,
            "greater than or equal to 1",
        ),
        (
            "pipelines.test.split.max_duration_s",
            3601,
            "less than or equal to 3600",
        ),
        (
            "pipelines.test.split.max_size_bytes",
            999,
            "greater than or equal to 1000",
        ),
        (
            "pipelines.test.split.max_size_bytes",
            1000000001,
            "less than or equal to 1000000000",
        ),
        (
            "pipelines.test.include_topics",
            ["/valid_topic", "invalid_topic"],
            "must be a list of ROS topic names starting with '/'",
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

    with pytest.raises(ValueError, match=rf"{err_msg}"):
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
        ValueError,
        match=re.escape(
            "Invalid pipeline parameter name: 'pipelines.invalid'. Expected 'pipelines.<pipeline_name>.<subkey>'"
        ),
    ):
        Recorder(
            parameter_overrides=as_overrides(storage_dict, [invalid_pipeline_param])
        )


def test_pipeline_missing_max_size():
    """Test that a pipeline without split_max_size_bytes (optional) works."""
    storage_dict = storage_params()
    pipeline_params_list = [
        Parameter(
            "pipelines.test.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.test.split.max_duration_s",
            Parameter.Type.INTEGER,
            10,
        ),
    ]
    try:
        node = Recorder(
            parameter_overrides=as_overrides(storage_dict, pipeline_params_list)
        )
        node.destroy_node()
    except Exception as e:
        pytest.fail(f"Unexpected error for valid pipeline config: {e}")


def test_pipeline_missing_max_duration():
    """Test that missing required split_max_duration_s fails."""
    storage_dict = storage_params()
    pipeline_params_list_missing_required = [
        Parameter(
            "pipelines.test.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
    ]
    with pytest.raises(ValueError, match="split.max_duration_s"):
        Recorder(
            parameter_overrides=as_overrides(
                storage_dict, pipeline_params_list_missing_required
            )
        )


def test_pipeline_invalid_filename_mode():
    """Test that an invalid filename mode raises an error."""
    storage_dict = storage_params()
    pipeline_params_list = [
        Parameter(
            "pipelines.test.include_topics",
            Parameter.Type.STRING_ARRAY,
            ["/test/topic"],
        ),
        Parameter(
            "pipelines.test.split.max_duration_s",
            Parameter.Type.INTEGER,
            10,
        ),
        Parameter(
            "pipelines.test.filename_mode",
            Parameter.Type.STRING,
            "invalid_mode",
        ),
    ]
    with pytest.raises(ValueError, match="Input should be 'timestamp' or 'counter'"):
        Recorder(parameter_overrides=as_overrides(storage_dict, pipeline_params_list))
