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

"""Utility functions for the ros2_reduct_agent package."""

import asyncio
import re
from asyncio import AbstractEventLoop


def get_or_create_event_loop() -> AbstractEventLoop:
    """Get the current event loop or create a new one if none exists."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def parse_bytes_with_si_units(value: str | int | float) -> int:
    """Parse byte values that can be specified as integers or with SI units."""
    if isinstance(value, (int, float)):
        return int(value)

    if not isinstance(value, str):
        raise ValueError(f"Value must be int, float, or str, got {type(value)}")

    # Match number with optional SI unit
    value = value.strip().upper()
    pattern = r"^(-?\d+(?:\.\d+)?)\s*([KMGTPB]?B)?$"
    match = re.match(pattern, value)

    if not match:
        raise ValueError(
            f"Invalid byte value format: '{value}'. "
            "Expected format: number with optional SI unit (e.g., '1KB', '5MB', '1GB')"
        )

    number_str, unit = match.groups()
    number = float(number_str)
    unit = unit or "B"

    si_multipliers = {
        "B": 1,
        "KB": 1_000,
        "MB": 1_000_000,
        "GB": 1_000_000_000,
    }

    if unit not in si_multipliers:
        raise ValueError(
            f"Unsupported unit: '{unit}'. "
            f"Supported units: {list(si_multipliers.keys())}"
        )

    result = int(number * si_multipliers[unit])

    if result < 0:
        raise ValueError(f"Byte value must be non-negative, got {result}")

    return result
