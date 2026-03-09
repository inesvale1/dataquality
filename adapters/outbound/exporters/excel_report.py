from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from openpyxl.utils import get_column_letter

def build_section_df(rows) -> pd.DataFrame:
    """Build a section DataFrame with either 3 or 4 columns."""
    if rows and len(rows[0]) == 4:
        columns = ["Indicator", "Category", "Description", "Value"]
    else:
        columns = ["Indicator", "Description", "Value"]
    return pd.DataFrame(rows, columns=columns)


def save_excel_report(
    base_folder: Path,
    schema_name: str,
    sections: Dict[str, pd.DataFrame],
    file_prefix: str = "issues_metadados",
) -> Path:
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    file_name_out = (
        Path(base_folder)
        / schema_name_upper
        / f"{file_prefix}_{schema_name}_{timestamp}.xlsx"
    )

    file_name_out.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(file_name_out, engine="openpyxl", mode="w") as writer:
        for sheet_name, df in sections.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            _autosize_worksheet_columns(writer, sheet_name, df)

    return file_name_out


def _autosize_worksheet_columns(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    worksheet = writer.sheets.get(sheet_name)
    if worksheet is None:
        return

    for idx, column_name in enumerate(df.columns, start=1):
        series = df[column_name] if column_name in df.columns else pd.Series(dtype=str)
        max_length = len(str(column_name))
        if not series.empty:
            cell_lengths = series.fillna("").astype(str).map(len)
            if not cell_lengths.empty:
                max_length = max(max_length, int(cell_lengths.max()))

        adjusted_width = min(max(max_length + 2, 10), 80)
        worksheet.column_dimensions[get_column_letter(idx)].width = adjusted_width
