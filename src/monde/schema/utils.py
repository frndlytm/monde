import math
import re


def drop_nulls(d: dict) -> dict:
    return {
        k: v for k, v in d.items()
        if not ((isinstance(v, float) and math.isnan(v)) or v is None)
    }


def precisionOf(x: float) -> int:
    """Get the number of numeric characters in the string as a float prceision"""
    return sum([c.isnumeric() for c in str(x)])


def title_case(name: str, sep: str = "") -> str:
    """Format the name as a title"""
    name, _ = re.subn(r"[\.\-\_]", ".", name)
    parts = (part.capitalize() for part in name.split("."))
    return sep.join(list(parts))
