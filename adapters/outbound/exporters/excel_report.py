from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

def build_section_df(rows) -> pd.DataFrame:
    """Build a simple (Indicator, Description, Value) section DataFrame."""
    return pd.DataFrame(rows, columns=["Indicator", "Description", "Value"])


def save_excel_report(base_folder: Path, scheme_name: str, sections: Dict[str, pd.DataFrame]) -> Path:
    """Write the Excel report for a schema.

    Output file name: issues_metadados_<scheme>.xlsx in the same folder as the input
    metadados_<scheme>.csv (as per current notebook behavior).
    """
    scheme_name_upper = scheme_name[:1].upper() + scheme_name[1:]
    #file_name_out = Path(base_folder) / f"{scheme_name_upper}/issues_metadados_{scheme_name}.xlsx"

    #file_name_out = (
    #    Path(base_folder)
    #    .relative_to(Path.cwd())
    #    /scheme_name_upper
    #    / f"issues_metadados_{scheme_name}.xlsx" )
    file_name_out = (
        Path(base_folder)
        / scheme_name_upper
        / f"issues_metadados_{scheme_name}.xlsx"
    )

    file_name_out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(file_name_out, engine="openpyxl", mode="w") as writer:
        for sheet_name, df in sections.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return file_name_out
