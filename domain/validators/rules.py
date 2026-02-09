from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

import pandas as pd


@dataclass(frozen=True)
class Rule:
    code: str
    desc: str
    check: Callable[[pd.DataFrame], pd.Series]


def apply_rules(df: pd.DataFrame, rules: Iterable[Rule]) -> pd.DataFrame:
    rows = []
    for rule in rules:
        mask = rule.check(df)
        if mask is None or mask.empty:
            continue
        for _, row in df[mask].iterrows():
            rows.append(
                {
                    "rule": rule.code,
                    "desc": rule.desc,
                    "owner": row.get("OWNER", ""),
                    "table": row.get("TABLE_NAME", ""),
                    "column": row.get("COLUMN_NAME", ""),
                    "constraint_name": "",
                    "length": "",
                    "limit": "",
                    "data_type": row.get("DATA_TYPE", ""),
                }
            )

    cols = ["rule", "desc", "owner", "table", "column", "constraint_name", "length", "limit", "data_type"]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
