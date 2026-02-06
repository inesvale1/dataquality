from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class ValidationConfig:
    """Configuration for naming/length rules."""

    max_table_len: int = 30
    max_column_len: int = 30

    # Allowed column name prefixes (3 letters + underscore)
    prefix_names: List[str] = field(
        default_factory=lambda: [
            "COD_",
            "DAT_",
            "DSC_",
            "NOM_",
            "NUM_",
            "QTD_",
            "SEQ_",
            "SIT_",
            "STA_",
            "TXT_",
            "TIP_",
            "TOT_",
            "VLR_",
            "BIN_",
            "HOR_",
            "XML_",
        ]
    )

    # Constraint naming patterns
    pk_prefix: str = "PK_"
    fk_prefix: str = "FK_"
    unique_suffix_regex: str = r".*_UK(\d+)?$"  # ends with _UK or _UK<digits>
