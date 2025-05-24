import re


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
            f"Unsupported unit: '{unit}'. Supported units: {list(si_multipliers.keys())}"
        )

    result = int(number * si_multipliers[unit])

    if result < 0:
        raise ValueError(f"Byte value must be non-negative, got {result}")

    return result
