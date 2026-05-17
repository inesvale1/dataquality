from __future__ import annotations

from dataclasses import dataclass, field

_DEFAULT_METADATA_WEIGHTS: dict[str, float] = {
    "MQID001": 3.0,
    "MQID002": 2.0,
    "MQID003": 6.0,
    "MQID004": 3.0,
    "MQID005": 10.0,
    "MQID006": 7.0,
    "MQID007": 7.0,
    "MQID008": 5.0,
    "MQID009": 8.0,
    "MQID010": 10.0,
    "MQID011": 10.0,
    "MQID012": 12.0,
    "MQID013": 10.0,
    "MQID014": 7.0,
}

_DEFAULT_DQ_WEIGHTS: dict[str, float] = {
    "Format Conformity": 40.0,
    "Uniqueness": 30.0,
    "Redundancy detection": 20.0,
    "MQID015": 10.0,
}


@dataclass
class ScoringConfig:
    metadata_metric_weights: dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_METADATA_WEIGHTS)
    )
    data_quality_metric_weights: dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_DQ_WEIGHTS)
    )
    mddq_phase_weight: float = 30.0
    ddq_phase_weight: float = 70.0
