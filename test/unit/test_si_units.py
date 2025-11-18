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


"""Test SI unit parsing functionality."""

import pytest

from reductstore_agent.models import PipelineConfig, parse_bytes_with_si_units


class TestSIUnitParsing:
    """Test cases for SI unit parsing function."""

    def test_parse_integer_values(self):
        """Test parsing plain integer values."""
        assert parse_bytes_with_si_units(1024) == 1024
        assert parse_bytes_with_si_units(5000000) == 5000000
        assert parse_bytes_with_si_units(0) == 0

    def test_parse_float_values(self):
        """Test parsing plain float values."""
        assert parse_bytes_with_si_units(1024.0) == 1024
        assert parse_bytes_with_si_units(1024.5) == 1024

    def test_parse_string_integers(self):
        """Test parsing string representations of integers."""
        assert parse_bytes_with_si_units("1024") == 1024
        assert parse_bytes_with_si_units("5000000") == 5000000

    def test_parse_si_units_case_insensitive(self):
        """Test parsing SI units with case insensitive matching."""
        assert parse_bytes_with_si_units("1KB") == 1000
        assert parse_bytes_with_si_units("1kb") == 1000
        assert parse_bytes_with_si_units("1Kb") == 1000
        assert parse_bytes_with_si_units("1MB") == 1_000_000
        assert parse_bytes_with_si_units("1GB") == 1_000_000_000

    def test_parse_decimal_si_units(self):
        """Test parsing decimal values with SI units."""
        assert parse_bytes_with_si_units("2.5GB") == 2_500_000_000
        assert parse_bytes_with_si_units("1.5MB") == 1_500_000
        assert parse_bytes_with_si_units("0.5KB") == 500

    def test_parse_with_whitespace(self):
        """Test parsing values with whitespace."""
        assert parse_bytes_with_si_units(" 1KB ") == 1000
        assert parse_bytes_with_si_units("1 MB") == 1_000_000
        assert parse_bytes_with_si_units("  2.5  GB  ") == 2_500_000_000

    def test_parse_bytes_unit(self):
        """Test explicit bytes unit."""
        assert parse_bytes_with_si_units("1024B") == 1024
        assert parse_bytes_with_si_units("1024b") == 1024

    def test_invalid_formats(self):
        """Test error handling for invalid formats."""
        with pytest.raises(ValueError, match="Invalid byte value format"):
            parse_bytes_with_si_units("invalid")

        with pytest.raises(ValueError, match="Invalid byte value format"):
            parse_bytes_with_si_units("1.2.3MB")

        with pytest.raises(ValueError, match="Invalid byte value format"):
            parse_bytes_with_si_units("MB")

        with pytest.raises(ValueError, match="Invalid byte value format"):
            parse_bytes_with_si_units("1XB")

    def test_negative_values(self):
        """Test error handling for negative values."""
        with pytest.raises(ValueError, match="Byte value must be non-negative"):
            parse_bytes_with_si_units("-1KB")

    def test_invalid_types(self):
        """Test error handling for invalid input types."""
        with pytest.raises(ValueError, match="Value must be int, float, or str"):
            parse_bytes_with_si_units(None)

        with pytest.raises(ValueError, match="Value must be int, float, or str"):
            parse_bytes_with_si_units([])


class TestPipelineConfigSIUnits:
    """Test PipelineConfig with SI unit parsing."""

    def test_pipeline_config_with_si_units(self):
        """Test creating PipelineConfig with SI unit strings."""
        config_data = {
            "split.max_duration_s": 10,
            "split.max_size_bytes": "5MB",
            "spool_max_size_bytes": "10MB",
            "include_topics": ["/test/topic"],
            "filename_mode": "timestamp",
        }

        config = PipelineConfig(**config_data)
        assert config.split_max_size_bytes == 5_000_000
        assert config.spool_max_size_bytes == 10_000_000

    def test_pipeline_config_with_integer_values(self):
        """Test backward compatibility with integer values."""
        config_data = {
            "split.max_duration_s": 10,
            "split.max_size_bytes": 5000000,
            "spool_max_size_bytes": 10000000,
            "include_topics": ["/test/topic"],
            "filename_mode": "timestamp",
        }

        config = PipelineConfig(**config_data)
        assert config.split_max_size_bytes == 5_000_000
        assert config.spool_max_size_bytes == 10_000_000

    def test_pipeline_config_mixed_formats(self):
        """Test mixing SI units and integer values."""
        config_data = {
            "split.max_duration_s": 10,
            "split.max_size_bytes": "5MB",
            "spool_max_size_bytes": 10485760,
            "include_topics": ["/test/topic"],
            "filename_mode": "timestamp",
        }

        config = PipelineConfig(**config_data)
        assert config.split_max_size_bytes == 5_000_000
        assert config.spool_max_size_bytes == 10485760

    def test_pipeline_config_invalid_si_unit(self):
        """Test error handling for invalid SI units in config."""
        config_data = {
            "split.max_duration_s": 10,
            "split.max_size_bytes": "5XB",
            "spool_max_size_bytes": "10MB",
            "include_topics": ["/test/topic"],
            "filename_mode": "timestamp",
        }

        with pytest.raises(ValueError, match="Invalid byte value format"):
            PipelineConfig(**config_data)
