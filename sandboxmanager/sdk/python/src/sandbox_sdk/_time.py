from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

_FRACTION = re.compile(r"\.(\d+)")


def parse_time(value: Optional[str]) -> Optional[datetime]:
    """Parse the API's RFC 3339 timestamps.

    Go emits a trailing "Z" and up to 9 fractional digits, neither of which
    datetime.fromisoformat accepts before Python 3.11.
    """
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    value = _FRACTION.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), value, count=1)
    return datetime.fromisoformat(value)
