from __future__ import annotations

from typing import Union

Number = Union[int, float]


def safe_iqmd(numerator: Number, denominator: Number) -> float:
    """Compute an IQMD indicator safely.

    Business rule: if the denominator is zero (or falsy), the indicator is treated as
    "not applicable" and returned as 0.0.

    Note: if you prefer returning None/NaN for N/A, change this here centrally.
    """
    if not denominator:
        return 0.0
    return float((1 - (numerator / denominator)) * 100)
