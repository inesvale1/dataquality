from __future__ import annotations

import pandas as pd

from dataquality.domain.config.metadata_metric_config import METADATA_INDICATOR_SPECS
from dataquality.domain.config.scoring_config import ScoringConfig


def compute_mddq(df_metrics: pd.DataFrame, weights: dict[str, float]) -> float | None:
    """Weighted average of MQID metric values (0-100 scale).

    Only indicators present in both `weights` and `df_metrics` contribute.
    Returns None when no indicator can be matched.
    """
    if df_metrics.empty or not weights:
        return None

    total_w = 0.0
    total_wv = 0.0
    for indicator, weight in weights.items():
        rows = df_metrics[df_metrics["Indicator"] == indicator]
        if rows.empty:
            continue
        raw = str(rows["Value"].iloc[0])
        try:
            value = float(raw)
        except (ValueError, TypeError):
            continue
        total_w += weight
        total_wv += weight * value

    if total_w == 0.0:
        return None
    return total_wv / total_w


def compute_ddq(df_metrics: pd.DataFrame, weights: dict[str, float]) -> float | None:
    """Weighted average of data quality metric types (0-100 scale).

    For each metric type the average of CALCULATED rows is used.
    Returns None when no metric type can be matched.
    """
    if df_metrics.empty or not weights:
        return None

    calculated = df_metrics[
        df_metrics["Status"].astype(str).str.startswith("CALCULATED", na=False)
    ].copy()
    if calculated.empty:
        return None

    calculated["_v"] = pd.to_numeric(calculated["Value"], errors="coerce")
    avg_by_type: dict[str, float] = (
        calculated.groupby("Metric")["_v"].mean().dropna().to_dict()
    )

    total_w = 0.0
    total_wv = 0.0
    for metric_type, weight in weights.items():
        avg = avg_by_type.get(metric_type)
        if avg is None:
            continue
        total_w += weight
        total_wv += weight * avg

    if total_w == 0.0:
        return None
    return total_wv / total_w


def compute_schema_score(
    mddq: float | None,
    ddq: float | None,
    config: ScoringConfig,
) -> float | None:
    """Combined schema quality score (0-100).

    Formula: (mddq_weight * MDDQ + ddq_weight * DDQ) / (mddq_weight + ddq_weight).
    Phases whose score is None are excluded and their weight is redistributed.
    """
    parts: list[tuple[float, float]] = []
    if mddq is not None:
        parts.append((config.mddq_phase_weight, mddq))
    if ddq is not None:
        parts.append((config.ddq_phase_weight, ddq))

    if not parts:
        return None

    total_w = sum(w for w, _ in parts)
    if total_w == 0.0:
        return None
    return sum(w * v for w, v in parts) / total_w


def build_mddq_scores_df(
    df_metrics: pd.DataFrame,
    weights: dict[str, float],
    mddq: float | None,
) -> pd.DataFrame:
    """Build the QUALITY_SCORES breakdown DataFrame for the model quality phase."""
    desc_by_indicator = {s.indicator: s.description for s in METADATA_INDICATOR_SPECS}
    dim_by_indicator: dict[str, str] = {}
    if not df_metrics.empty and "Dimension" in df_metrics.columns:
        dim_by_indicator = df_metrics.set_index("Indicator")["Dimension"].to_dict()

    rows: list[dict] = []
    total_w = 0.0
    for indicator, weight in weights.items():
        metric_rows = df_metrics[df_metrics["Indicator"] == indicator] if not df_metrics.empty else pd.DataFrame()
        raw = str(metric_rows["Value"].iloc[0]) if not metric_rows.empty else None
        try:
            value = float(raw) if raw is not None else None
        except (ValueError, TypeError):
            value = None

        total_w += weight
        rows.append({
            "ScoreType": "MDDQ",
            "Component": indicator,
            "Dimension": dim_by_indicator.get(indicator, ""),
            "Description": desc_by_indicator.get(indicator, ""),
            "Weight": round(weight, 4),
            "Value": round(value, 4) if value is not None else None,
        })

    rows.append({
        "ScoreType": "MDDQ",
        "Component": "MDDQ",
        "Dimension": "",
        "Description": "Metadata DQ Weighted Average",
        "Weight": round(total_w, 4),
        "Value": round(mddq, 4) if mddq is not None else None,
    })

    return pd.DataFrame(rows)


def build_ddq_scores_df(
    df_metrics: pd.DataFrame,
    weights: dict[str, float],
    ddq: float | None,
) -> pd.DataFrame:
    """Build the QUALITY_SCORES breakdown DataFrame for the data quality phase."""
    calculated = (
        df_metrics[
            df_metrics["Status"].astype(str).str.startswith("CALCULATED", na=False)
        ].copy()
        if not df_metrics.empty
        else pd.DataFrame()
    )

    dim_by_metric: dict[str, str] = {}
    if not calculated.empty and "Dimension" in calculated.columns:
        dim_by_metric = calculated.groupby("Metric")["Dimension"].first().to_dict()

    if not calculated.empty:
        calculated["_v"] = pd.to_numeric(calculated["Value"], errors="coerce")
        avg_by_type: dict[str, float | None] = (
            calculated.groupby("Metric")["_v"].mean().dropna().to_dict()
        )
    else:
        avg_by_type = {}

    rows: list[dict] = []
    total_w = 0.0
    for metric_type, weight in weights.items():
        avg = avg_by_type.get(metric_type)
        total_w += weight
        rows.append({
            "ScoreType": "DDQ",
            "Component": metric_type,
            "Dimension": dim_by_metric.get(metric_type, ""),
            "Description": f"Avg value for metric type '{metric_type}'",
            "Weight": round(weight, 4),
            "Value": round(avg, 4) if avg is not None else None,
        })

    rows.append({
        "ScoreType": "DDQ",
        "Component": "DDQ",
        "Dimension": "",
        "Description": "Data Quality Weighted Average",
        "Weight": round(total_w, 4),
        "Value": round(ddq, 4) if ddq is not None else None,
    })

    return pd.DataFrame(rows)


def build_schema_score_rows(
    mddq: float | None,
    ddq: float | None,
    schema_score: float | None,
    config: ScoringConfig,
) -> list[dict]:
    """Return a single row with the combined schema score.

    MDDQ and DDQ values are already present in their own ScoreType blocks,
    so repeating them here would create redundant rows with the same value.
    """
    return [
        {
            "ScoreType": "SCHEMA_SCORE",
            "Component": "SCHEMA_SCORE",
            "Dimension": "",
            "Description": "Overall schema quality score",
            "Weight": None,
            "Value": round(schema_score, 4) if schema_score is not None else None,
        }
    ]
