from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

def build_section_df(rows) -> pd.DataFrame:
    """Build a section DataFrame with either 3 or 4 columns."""
    if rows and len(rows[0]) == 4:
        columns = ["Indicator", "Category", "Description", "Value"]
    else:
        columns = ["Indicator", "Description", "Value"]
    return pd.DataFrame(rows, columns=columns)


def save_excel_report(base_folder: Path, schema_name: str, sections: Dict[str, pd.DataFrame]) -> Path:
    """Write the Excel report for a schema.

    Output file name: issues_metadados_<schema>.xlsx in the same folder as the input
    metadados_<schema>.csv (as per current notebook behavior).
    """
    schema_name_upper = schema_name[:1].upper() + schema_name[1:]
    #file_name_out = Path(base_folder) / f"{schema_name_upper}/issues_metadados_{schema_name}.xlsx"

    #file_name_out = (
    #    Path(base_folder)
    #    .relative_to(Path.cwd())
    #    /schema_name_upper
    #    / f"issues_metadados_{schema_name}.xlsx" )
    file_name_out = (
        Path(base_folder)
        / schema_name_upper
        / f"issues_metadados_{schema_name}.xlsx"
    )

    file_name_out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(file_name_out, engine="openpyxl", mode="w") as writer:
        for sheet_name, df in sections.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return file_name_out
