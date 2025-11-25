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

"""Test Recorder node parameter validation and configuration."""

import re

import pytest
from rclpy.parameter import Parameter

from reductstore_agent.models import LabelTopicConfig
from reductstore_agent.recorder import Recorder


def storage_params():
    """Return a dictionary of valid storage parameters."""
    return {
        "url": "http://localhost:8383",
        "api_token": "test_token",
        "bucket": "test_bucket",
    }


def pipeline_params():
    """Return a list of valid pipeline parameters."""
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
            "incremental",
        ),
    ]


def downsampling_params_stride():
    """Return a list of valid downsampling parameters for 'stride'."""
    return [
        Parameter(
            "pipelines.test.downsampling_mode",
            Parameter.Type.STRING,
            "stride",
        ),
        Parameter(
            "pipelines.test.stride_n",
            Parameter.Type.INTEGER,
            5,
        ),
    ]


def downsampling_params_max_rate():
    """Return a list of valid downsampling params for 'max_rate'."""
    return [
        Parameter(
            "pipelines.test.downsampling_mode",
            Parameter.Type.STRING,
            "max_rate",
        ),
        Parameter(
            "pipelines.test.max_rate_hz",
            Parameter.Type.DOUBLE,
            10.0,
        ),
    ]


def downsampling_params_none():
    """Return a list of valid downsampling params for 'none'."""
    return [
        Parameter(
            "pipelines.test.downsampling_mode",
            Parameter.Type.STRING,
            "none",
        ),
    ]


def output_format_params_cdr():
    """Return a list of valid output_format params for 'none'."""
    return [Parameter("pipelines.test.output_format", Parameter.Type.STRING, "cdr")]


def output_format_params_mcap():
    """Return a list of valid output_format params for 'mcap'."""
    return [Parameter("pipelines.test.output_format", Parameter.Type.STRING, "mcap")]


def dynamic_label_params():
    """Return a list of valid parameters for LabelStateTracker."""
    return [
        # --- Label 0: LAST Mode ---
        Parameter(
            "pipelines.test.labels.0.topic",
            Parameter.Type.STRING,
            "/mission_info",
        ),
        Parameter(
            "pipelines.test.labels.0.mode",
            Parameter.Type.STRING,
            "last",
        ),
        Parameter(
            "pipelines.test.labels.0.fields.mission_id",
            Parameter.Type.STRING,
            "mission_id",
        ),
        Parameter(
            "pipelines.test.labels.0.fields.operator",
            Parameter.Type.STRING,
            "operator",
        ),
        # --- Label 1: MAX Mode ---
        Parameter(
            "pipelines.test.labels.1.topic",
            Parameter.Type.STRING,
            "/telemetry",
        ),
        Parameter(
            "pipelines.test.labels.1.mode",
            Parameter.Type.STRING,
            "max",
        ),
        Parameter(
            "pipelines.test.labels.1.fields.max_speed",
            Parameter.Type.STRING,
            "speed",
        ),
        # --- Label 2: FIRST Mode ---
        Parameter(
            "pipelines.test.labels.2.topic",
            Parameter.Type.STRING,
            "/startup_config",
        ),
        Parameter(
            "pipelines.test.labels.2.mode",
            Parameter.Type.STRING,
            "first",
        ),
        Parameter(
            "pipelines.test.labels.2.fields.start_volt",
            Parameter.Type.STRING,
            "voltage",
        ),
    ]


def as_overrides(
    storage_dict,
    pipeline_params=None,
    downsampling_params=None,
    output_format_params=None,
    label_params=None,
):
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
    if downsampling_params:
        for param in downsampling_params:
            overrides.append(param)
    if output_format_params:
        for param in output_format_params:
            overrides.append(param)
    if label_params:
        for param in label_params:
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


def get_all_downsampling_modes():
    """Return a list of tuples: (mode_name, params_list_function)."""
    return [
        ("none,", downsampling_params_none),
        ("stride", downsampling_params_stride),
        ("max_rate", downsampling_params_max_rate),
    ]


def get_all_output_modes():
    """Return a list of tuples: (output_format, param_list_function)."""
    return [
        ("mcap", output_format_params_mcap),
        ("cdr", output_format_params_cdr),
    ]


@pytest.mark.parametrize("mode_name, downsampling_func", get_all_downsampling_modes())
def test_recorder_valid_all_downsampling_modes(mode_name, downsampling_func):
    """Test that the Recorder node can be created with all downsampling_modes."""
    downsampling_params = downsampling_func()
    node = Recorder(
        parameter_overrides=as_overrides(
            storage_params(), pipeline_params(), downsampling_params
        )
    )
    assert node.get_name() == "recorder"
    node.destroy_node()


@pytest.mark.parametrize("output_format, output_format_func", get_all_output_modes())
@pytest.mark.parametrize("mode_name, downsampling_func", get_all_downsampling_modes())
def test_recorder_valid_all_configuration(
    output_format, output_format_func, mode_name, downsampling_func
):
    """Test that Recoder node can be created with all valid configurations."""
    downsampling_params = downsampling_func()
    output_params = output_format_func()
    labels = dynamic_label_params()
    node = Recorder(
        parameter_overrides=as_overrides(
            storage_params(),
            pipeline_params(),
            downsampling_params,
            output_params,
            labels,
        )
    )
    assert node.get_name() == "recorder"
    node.destroy_node()


@pytest.mark.parametrize("missing_key", ["url", "api_token", "bucket"])
def test_recorder_missing_storage_param(missing_key):
    """Test that the Recorder node raises an error if missing storage parameter."""
    params = storage_params()
    params.pop(missing_key)
    with pytest.raises(
        ValueError, match=rf"Missing parameter: 'storage\.{missing_key}'"
    ):
        Recorder(parameter_overrides=as_overrides(params))


@pytest.mark.parametrize("empty_key", ["url", "bucket"])
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
    ],
)
def test_recorder_invalid_pipeline_param(param_name, invalid_value, err_msg):
    """Raises an error if a pipeline parameter is invalid."""
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
    """Raises an error for pipeline parameters with invalid names."""
    storage_dict = storage_params()
    invalid_pipeline_param = Parameter(
        "pipelines.invalid", Parameter.Type.STRING, "something"
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid pipeline parameter name: 'pipelines.invalid'. "
            "Expected 'pipelines.<pipeline_name>.<subkey>'"
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
    with pytest.raises(
        ValueError, match="Input should be 'timestamp' or 'incremental'"
    ):
        Recorder(parameter_overrides=as_overrides(storage_dict, pipeline_params_list))


def test_recorder_valid_mcap_pipeline_params():
    """Test that the Recorder node accepts valid chunking and compression params."""
    extra_params = [
        Parameter("pipelines.test.chunk_size_bytes", Parameter.Type.INTEGER, 1_000_000),
        Parameter("pipelines.test.compression", Parameter.Type.STRING, "zstd"),
        Parameter(
            "pipelines.test.spool_max_size_bytes", Parameter.Type.INTEGER, 20_000_000
        ),
        Parameter("pipelines.test.enable_crcs", Parameter.Type.BOOL, True),
    ]
    full_pipeline_params = pipeline_params() + extra_params

    node = Recorder(
        parameter_overrides=as_overrides(storage_params(), full_pipeline_params)
    )
    assert node.get_name() == "recorder"
    node.destroy_node()


def test_recorder_valid_label_params():
    """Test that the Recorder node can be created with valid label parameters."""
    node = Recorder(
        parameter_overrides=as_overrides(
            storage_params(), pipeline_params(), label_params=dynamic_label_params()
        )
    )
    assert node.get_name() == "recorder"
    node.destroy_node()


def test_recorder_invalid_chunk_size():
    """Test that an invalid chunk size raises an error."""
    storage_dict = storage_params()
    pipeline_params_list = pipeline_params() + [
        Parameter("pipelines.test.chunk_size_bytes", Parameter.Type.INTEGER, 999),
    ]
    with pytest.raises(ValueError, match="greater than or equal to 1000"):
        Recorder(parameter_overrides=as_overrides(storage_dict, pipeline_params_list))


def test_recorder_invalid_spool_size():
    """Test that an invalid spool size raises a validation error."""
    storage_dict = storage_params()
    pipeline_params_list = pipeline_params() + [
        Parameter("pipelines.test.spool_max_size_bytes", Parameter.Type.INTEGER, 999),
    ]

    with pytest.raises(ValueError, match="greater than or equal to 1000"):
        Recorder(parameter_overrides=as_overrides(storage_dict, pipeline_params_list))


def test_recorder_invalid_compression():
    """Test that an invalid compression type raises a validation error."""
    storage_dict = storage_params()
    pipeline_params_list = pipeline_params() + [
        Parameter(
            "pipelines.test.compression", Parameter.Type.STRING, "invalid_compression"
        )
    ]

    with pytest.raises(
        ValueError, match="String should match pattern '.*(none|lz4|zstd).*'"
    ):
        Recorder(parameter_overrides=as_overrides(storage_dict, pipeline_params_list))


def test_recorder_invalid_enable_crcs():
    """Test that an invalid enable_crcs value raises a type/value error."""
    with pytest.raises(ValueError, match="'not_a_bool' do not agree"):
        Parameter("pipelines.test.enable_crcs", Parameter.Type.BOOL, "not_a_bool")


def test_recorder_valid_quota_and_block_params():
    """Test that valid quota and block size parameters are accepted."""
    storage_dict = storage_params()
    storage_dict["quota_type"] = "FIFO"
    storage_dict["quota_size"] = "1GB"
    storage_dict["max_block_size"] = "10MB"
    storage_dict["max_block_records"] = "1000"
    node = Recorder(parameter_overrides=as_overrides(storage_dict))
    assert node.get_name() == "recorder"
    node.destroy_node()


def test_recorder_invalid_quota_type():
    """Test that an invalid quota type raises a validation error."""
    storage_dict = storage_params()
    storage_dict["quota_type"] = "invalid_quota"
    with pytest.raises(ValueError, match="Invalid quota type: 'invalid_quota'"):
        Recorder(parameter_overrides=as_overrides(storage_dict))


def test_recorder_invalid_quota_size():
    """Test that an invalid quota size raises a validation error."""
    storage_dict = storage_params()
    storage_dict["quota_size"] = "invalid_size"
    with pytest.raises(ValueError, match="Invalid byte value format"):
        Recorder(parameter_overrides=as_overrides(storage_dict))


def test_recorder_invalid_max_block_size():
    """Test that an invalid max block size raises a validation error."""
    storage_dict = storage_params()
    storage_dict["max_block_size"] = "invalid_size"
    with pytest.raises(ValueError, match="Invalid byte value format"):
        Recorder(parameter_overrides=as_overrides(storage_dict))


def test_recorder_invalid_max_block_records():
    """Test that an invalid max records per block raises a validation error."""
    storage_dict = storage_params()
    storage_dict["max_block_records"] = "invalid_size"
    with pytest.raises(ValueError, match="should be a valid integer"):
        Recorder(parameter_overrides=as_overrides(storage_dict))


DOWN_SAMPLING_INVALID_CASES = [
    (
        "pipelines.test.stride_n",
        Parameter.Type.INTEGER,
        1,
        "greater than or equal to 2",
    ),
    (
        "pipelines.test.max_rate_hz",
        Parameter.Type.DOUBLE,
        -5.0,
        r"max_rate_hz\n  Input should be greater than or equal to 0",
    ),
    (
        "pipelines.test.downsampling_mode",
        Parameter.Type.STRING,
        "invalid_mode",
        r"Input should be 'none', 'stride' or 'max_rate'",
    ),
]


@pytest.mark.parametrize(
    "param_name, param_type, invalid_value, err_msg",
    DOWN_SAMPLING_INVALID_CASES,
)
def test_recorder_invalid_downsampling_params(
    param_name, param_type, invalid_value, err_msg
):
    """Test that Recorder raises Error for invalid config."""
    invalid_param = Parameter(param_name, param_type, invalid_value)
    all_pipeline_params = pipeline_params() + [invalid_param]

    with pytest.raises(ValueError, match=rf"{err_msg}"):
        Recorder(
            parameter_overrides=as_overrides(storage_params(), all_pipeline_params)
        )


LABEL_INVALID_CASES = [
    (
        {"topic": "foo", "mode": "invalid_mode", "fields": {"k": "v"}},
        r"Input should be 'last', 'first' or 'max'",
    ),
    (
        {"topic": 123, "mode": "last", "fields": {"k": "v"}},
        r"topic\n  Input should be a valid string",
    ),
    (
        {"topic": "foo", "mode": "last", "fields": "not_a_map"},
        r"fields\n  Input should be a valid dictionary",
    ),
]


@pytest.mark.parametrize("payload, err_msg", LABEL_INVALID_CASES)
def test_label_topic_config_invalid_cases(payload, err_msg):
    """Test if invalid LabelTopicConfig file raises an error."""
    with pytest.raises(ValueError, match=rf"{err_msg}"):
        LabelTopicConfig(**payload)
